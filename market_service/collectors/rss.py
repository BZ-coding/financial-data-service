#!/usr/bin/env python3
"""
RSS订阅采集器
复用并优化现有的 rss-subscriber 代码
"""

import sys
import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

# 使用akshare_venv环境（RSS依赖feedparser）
AKSHARE_VENV = Path("/vol1/@apphome/trim.openclaw/data/workspace/akshare_venv")
if AKSHARE_VENV.exists():
    site_packages = list(AKSHARE_VENV.glob("lib/python*/site-packages"))[0]
    sys.path.insert(0, str(site_packages))

import feedparser
import akshare as ak

from .base import BaseCollector, CollectResult

logger = logging.getLogger(__name__)

TZ = timezone(timedelta(hours=8))

class RSSCollector(BaseCollector):
    """RSS及akshare新闻采集器（统一接口）"""

    def __init__(self, config_path: str = None, rate_limit_per_second: int = 1):
        super().__init__(rate_limit_per_second * 60)
        if config_path is None:
            # 正确路径：collectors/rss.py -> parent.parent = market_service, 然后 config/rss_config.json
            config_path = Path(__file__).parent.parent / "config" / "rss_config.json"
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if self.config_path.exists():
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        logger.warning(f"RSS配置文件不存在: {self.config_path}，使用默认空配置")
        return {"sources": []}

    async def collect(self, symbol: str, data_type: str, **kwargs) -> CollectResult:
        """注意：RSS采集器不按symbol区分，而是按source"""
        start = datetime.now(TZ)

        # data_type 应为 "news"
        if data_type != "news":
            return CollectResult(
                success=False,
                data=None,
                source="rss",
                symbol=None,
                data_type="news",
                error="RSS只支持news类型",
                duration_seconds=(datetime.now(TZ) - start).total_seconds()
            )

        # symbol参数作为source名称过滤（可选）
        source_name = kwargs.get('source_name') or symbol
        limit = kwargs.get('limit', 20)

        try:
            # 如果symbol是source名称，只采集该源
            if source_name and source_name != "*":
                sources = [s for s in self.config['sources'] if s['name'] == source_name]
                if not sources:
                    return CollectResult(False, None, "rss", None, "news", f"源不存在: {source_name}")
            else:
                sources = self.config['sources']

            all_items = []
            for src in sources:
                items = await self._fetch_source(src, limit)
                # 按source配置的关键词过滤
                keywords = src.get('keywords', [])
                if keywords:
                    filtered = []
                    for item in items:
                        text = (item['title'] + ' ' + item.get('summary', '')).lower()
                        if any(k.lower() in text for k in keywords):
                            filtered.append(item)
                    all_items.extend(filtered)
                else:
                    all_items.extend(items)

            # 去重（基于link）
            seen = set()
            unique = []
            for item in all_items:
                link = item.get('link', '')
                if link and link not in seen:
                    seen.add(link)
                    unique.append(item)

            return CollectResult(
                success=True,
                data=unique[:limit],
                source="rss",
                symbol=None,
                data_type="news",
                duration_seconds=(datetime.now(TZ) - start).total_seconds()
            )

        except Exception as e:
            logger.error(f"RSS采集异常: {e}")
            return CollectResult(
                success=False,
                data=None,
                source="rss",
                symbol=None,
                data_type="news",
                error=str(e),
                duration_seconds=(datetime.now(TZ) - start).total_seconds()
            )

    async def collect_batch(self, symbols: List[str], data_type: str, **kwargs) -> List[CollectResult]:
        """RSS不支持批量symbol，执行N次collect"""
        tasks = [self.collect(sym, data_type, **kwargs) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        final = []
        for r in results:
            if isinstance(r, Exception):
                final.append(CollectResult(False, None, "rss", None, "news", error=str(r)))
            else:
                final.append(r)
        return final

    async def _fetch_source(self, source: Dict[str, Any], limit: int) -> List[Dict[str, Any]]:
        """获取单个源的内容"""
        src_type = source.get('type', 'rss')
        if src_type == 'akshare':
            return await self._fetch_akshare_source(source)
        else:
            return await self._fetch_rss_source(source, limit)

    async def _fetch_akshare_source(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        """使用akshare模块获取数据"""
        module_name = source.get('module')
        if not module_name or not hasattr(ak, module_name):
            logger.warning(f"akshare无模块: {module_name}")
            return []

        try:
            await self._acquire_token()
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, getattr(ak, module_name))

            if df is None or df.empty:
                return []

            items = []
            # 字段映射（根据模块调整）
            for _, row in df.head(50).iterrows():
                title = self._extract_field(row, ['新闻标题', '标题', 'title', 'name'])
                summary = self._extract_field(row, ['新闻内容', '内容', '摘要', 'summary', 'content'])
                link = self._extract_field(row, ['新闻链接', '链接', 'url', 'link'])
                published = self._extract_field(row, ['发布时间', '时间', 'date', 'published'])

                if title and title != 'nan':
                    items.append({
                        'title': str(title),
                        'summary': str(summary) if summary else '',
                        'link': str(link),
                        'published': str(published),
                        'source': source['name']
                    })
            return items
        except Exception as e:
            logger.error(f"akshare源 {source['name']} 失败: {e}")
            return []

    def _extract_field(self, row: Any, possible_keys: List[str]) -> Any:
        """从pandas row中提取字段"""
        for key in possible_keys:
            if key in row:
                return row[key]
        return None

    async def _fetch_rss_source(self, source: Dict[str, Any], limit: int) -> List[Dict[str, Any]]:
        """HTTP获取RSS/JSON源"""
        url = source.get('url')
        if not url:
            return []

        try:
            await self._acquire_token()
            import urllib.request, ssl, re, html
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; RSS-Bot/1.0)'}
            ctx = ssl._create_unverified_context()
            req = urllib.request.Request(url, headers=headers)

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, context=ctx, timeout=15)
            )
            raw = response.read()
            content_type = response.headers.get('Content-Type', '')

            # 解码
            encoding = 'utf-8'
            if 'charset=' in content_type:
                encoding = content_type.split('charset=')[1].split(';')[0].strip()
            content = raw.decode(encoding, errors='ignore')

            # 判断格式
            if 'application/json' in content_type or content.strip().startswith('{'):
                return await self._parse_json_content(content, source['name'], limit)
            else:
                return await self._parse_rss_content(content, source['name'], limit)

        except Exception as e:
            logger.error(f"RSS源 {source['name']} 失败: {e}")
            return []

    async def _parse_rss_content(self, xml_content: str, source_name: str, limit: int) -> List[Dict[str, Any]]:
        """解析RSS/ATOM XML"""
        import re
        import html
        items = []
        pattern = r'<(item|entry)>(.*?)</\1>'
        matches = re.findall(pattern, xml_content, re.DOTALL | re.IGNORECASE)

        for _, item_xml in matches[:limit]:
            title_match = re.search(r'<title[^>]*>(.*?)</title>', item_xml, re.DOTALL | re.IGNORECASE)
            link_match = re.search(r'<link[^>]*>(.*?)</link>', item_xml, re.DOTALL | re.IGNORECASE)
            desc_match = re.search(r'<(description|summary|content)[^>]*>(.*?)</\1>', item_xml, re.DOTALL | re.IGNORECASE)
            date_match = re.search(r'<(pubDate|published|updated|dc:date)[^>]*>(.*?)</\1>', item_xml, re.DOTALL | re.IGNORECASE)

            if title_match:
                title = html.unescape(re.sub(r'<[^>]+>', '', title_match.group(1))).strip()
                link = ''
                if link_match:
                    link = html.unescape(re.sub(r'<[^>]+>', '', link_match.group(1)).strip())
                else:
                    href_match = re.search(r'<link[^>]*href=["\'](.*?)["\']', item_xml, re.IGNORECASE)
                    if href_match:
                        link = href_match.group(1)
                summary = html.unescape(re.sub(r'<[^>]+>', '', desc_match.group(1)).strip())[:300] if desc_match else ''
                published = html.unescape(re.sub(r'<[^>]+>', '', date_match.group(2)).strip()) if date_match else ''

                items.append({
                    'title': title,
                    'summary': summary,
                    'link': link,
                    'published': published,
                    'source': source_name
                })
        return items

    async def _parse_json_content(self, json_str: str, source_name: str, limit: int) -> List[Dict[str, Any]]:
        """解析JSON格式（新浪等）"""
        import re, json as _json
        items = []
        try:
            # 去除回调包装
            json_str = json_str.strip()
            if re.match(r'^[a-zA-Z_]\s*\(', json_str):
                json_str = re.sub(r'^[a-zA-Z_]\s*\(\s*', '', json_str, count=1)
                json_str = re.sub(r'\)\s*;\s*$', '', json_str)

            data = _json.loads(json_str)

            # 新浪结构
            if 'result' in data and 'data' in data['result']:
                for entry in data['result']['data'][:limit]:
                    title = entry.get('title', '')
                    if not title:
                        continue
                    link = entry.get('url', '')
                    if link and '\\/' in link:
                        link = link.encode().decode('unicode_escape', errors='ignore')
                    summary = entry.get('intro', entry.get('summary', ''))
                    summary = re.sub(r'<[^>]+>', '', str(summary))[:300] if summary else ''
                    published = entry.get('pubtime', entry.get('ctime', ''))

                    items.append({
                        'title': title.strip(),
                        'summary': summary,
                        'link': link,
                        'published': published,
                        'source': source_name
                    })
        except Exception as e:
            logger.error(f"JSON解析失败 {source_name}: {e}")
        return items

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = RSSCollector()
    result = asyncio.run(collector.collect("*", "news", limit=5))
    print(f"采集结果: {len(result.data)} 条")
    if result.success and result.data:
        print("第一条:", json.dumps(result.data[0], ensure_ascii=False, indent=2))
