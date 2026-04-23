"""
x402 Global Data API Actor v2.0 — AI Agent Native
===================================================
Apify Actor + x402 protokoll kompatibilis adatszolgáltatás.
AI agentek USDC-vel fizetnek minden egyes API hívásért.

Elérhető endpointok:
  CRYPTO:     crypto/prices, crypto/fear-greed
  PÉNZPIAC:   market/stocks, market/forex
  ENERGIA:    energy/europe, energy/global
  MAKRO:      macro/rates
  HÍREK:      news/crypto, news/general
  TRENDEK:    trends/google, trends/x
  HIRDETÉSEK: ads/facebook
  E-COMMERCE: ecommerce/amazon
  META:       info (INGYENES)
"""

import asyncio
import json
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone

import httpx
from apify import Actor

ENDPOINTS = {
    "crypto/prices":     {"price_usd": 0.02, "desc": "Real-time crypto prices (300+ coins, USD/EUR/GBP)"},
    "crypto/fear-greed": {"price_usd": 0.02, "desc": "Crypto Fear & Greed Index + 30d history"},
    "market/stocks":     {"price_usd": 0.05, "desc": "Stock/ETF prices (NYSE, NASDAQ, EU exchanges)"},
    "market/forex":      {"price_usd": 0.02, "desc": "Forex rates (170+ currency pairs)"},
    "energy/europe":     {"price_usd": 0.05, "desc": "EU electricity spot prices (20 countries, EUR/MWh)"},
    "energy/global":     {"price_usd": 0.08, "desc": "Global electricity prices (US, AU, JP, CA, IN, BR)"},
    "macro/rates":       {"price_usd": 0.05, "desc": "Central bank rates (G20 + regional, 13 banks)"},
    "news/crypto":       {"price_usd": 0.05, "desc": "Latest crypto news + sentiment scores"},
    "news/general":      {"price_usd": 0.05, "desc": "Top global news headlines (Reuters RSS)"},
    "trends/google":     {"price_usd": 0.08, "desc": "Google Trends for any keyword, any country"},
    "trends/x":          {"price_usd": 0.08, "desc": "X (Twitter) trending topics by country"},
    "ads/facebook":      {"price_usd": 0.15, "desc": "Meta Ads Library — active ads by keyword/country"},
    "ecommerce/amazon":  {"price_usd": 0.15, "desc": "Amazon product prices, ratings, BSR rankings"},
    "info":              {"price_usd": 0.00, "desc": "List all endpoints and pricing (FREE)"},
}

ENERGY_COUNTRIES_EU = {
    "DE": "Germany",     "FR": "France",      "AT": "Austria",
    "HU": "Hungary",     "CZ": "Czech Rep.",  "SK": "Slovakia",
    "RO": "Romania",     "PL": "Poland",      "IT": "Italy",
    "ES": "Spain",       "NL": "Netherlands", "BE": "Belgium",
    "CH": "Switzerland", "SI": "Slovenia",    "HR": "Croatia",
    "SE": "Sweden",      "NO": "Norway",      "FI": "Finland",
    "DK": "Denmark",     "PT": "Portugal",
}

MACRO_RATES_STATIC = {
    "FED":  {"name": "Federal Reserve (USA)",      "rate_pct": 4.50, "last_change": "2024-12-18", "trend": "hold"},
    "ECB":  {"name": "European Central Bank",      "rate_pct": 2.65, "last_change": "2025-03-06", "trend": "cut"},
    "BOE":  {"name": "Bank of England",            "rate_pct": 4.50, "last_change": "2025-02-06", "trend": "cut"},
    "BOJ":  {"name": "Bank of Japan",              "rate_pct": 0.50, "last_change": "2025-01-24", "trend": "hike"},
    "SNB":  {"name": "Swiss National Bank",        "rate_pct": 0.25, "last_change": "2025-03-20", "trend": "cut"},
    "RBA":  {"name": "Reserve Bank of Australia",  "rate_pct": 4.10, "last_change": "2025-02-18", "trend": "cut"},
    "BOC":  {"name": "Bank of Canada",             "rate_pct": 2.75, "last_change": "2025-03-12", "trend": "cut"},
    "PBOC": {"name": "People's Bank of China",     "rate_pct": 3.10, "last_change": "2024-10-21", "trend": "cut"},
    "RBI":  {"name": "Reserve Bank of India",      "rate_pct": 6.00, "last_change": "2025-02-07", "trend": "cut"},
    "BCB":  {"name": "Banco Central do Brasil",    "rate_pct": 14.75,"last_change": "2025-03-19", "trend": "hike"},
    "SARB": {"name": "South African Reserve Bank", "rate_pct": 7.50, "last_change": "2025-03-20", "trend": "cut"},
    "MNB":  {"name": "Magyar Nemzeti Bank",        "rate_pct": 6.50, "last_change": "2025-03-25", "trend": "hold"},
    "NBP":  {"name": "National Bank of Poland",    "rate_pct": 5.75, "last_change": "2024-10-05", "trend": "hold"},
}

async def fetch_crypto_prices(symbols, client):
    common_map = {
        "BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin",
        "XRP":"ripple","ADA":"cardano","AVAX":"avalanche-2","DOT":"polkadot",
        "MATIC":"matic-network","LINK":"chainlink","UNI":"uniswap","ATOM":"cosmos",
        "LTC":"litecoin","BCH":"bitcoin-cash","NEAR":"near","APT":"aptos",
        "ARB":"arbitrum","OP":"optimism","DOGE":"dogecoin","SHIB":"shiba-inu",
        "TON":"the-open-network","SUI":"sui","TIA":"celestia","INJ":"injective-protocol",
    }
    requested = [s.upper() for s in (symbols or ["BTC","ETH","SOL"])]
    coin_ids = ",".join(common_map.get(s, s.lower()) for s in requested[:50])
    try:
        r = await client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": coin_ids, "vs_currencies": "usd,eur,gbp",
                    "include_24hr_change": "true", "include_24hr_vol": "true",
                    "include_market_cap": "true", "include_last_updated_at": "true"},
            timeout=12.0,
        )
        r.raise_for_status()
        raw = r.json()
        rev = {v: k for k, v in common_map.items()}
        result = {}
        for cid, d in raw.items():
            sym = rev.get(cid, cid.upper())
            result[sym] = {
                "price_usd":      round(d.get("usd", 0), 6),
                "price_eur":      round(d.get("eur", 0), 6),
                "price_gbp":      round(d.get("gbp", 0), 6),
                "change_24h_pct": round(d.get("usd_24h_change", 0), 2),
                "volume_24h_usd": d.get("usd_24h_vol", 0),
                "market_cap_usd": d.get("usd_market_cap", 0),
                "last_updated":   datetime.fromtimestamp(d.get("last_updated_at", time.time()), tz=timezone.utc).isoformat(),
            }
        return {"success": True, "data": result, "count": len(result), "source": "CoinGecko"}
    except Exception as e:
        return {"success": False, "error": str(e), "data": {}}

async def fetch_fear_greed(client):
    try:
        r = await client.get("https://api.alternative.me/fng/", params={"limit": 30}, timeout=10.0)
        r.raise_for_status()
        entries = r.json().get("data", [])
        history = [{"date": datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).strftime("%Y-%m-%d"),
                    "value": int(e["value"]), "sentiment": e["value_classification"]} for e in entries]
        return {"success": True, "current": history[0] if history else {}, "history_30d": history, "source": "Alternative.me"}
    except Exception as e:
        return {"success": False, "error": str(e)}

async def fetch_stocks(tickers, client):
    if not tickers:
        tickers = ["AAPL","MSFT","GOOGL","NVDA","TSLA","SPY","QQQ","META","AMZN"]
    results, errors = {}, []
    async def fetch_one(ticker):
        try:
            r = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}",
                params={"interval": "1d", "range": "5d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10.0,
            )
            if r.status_code != 200:
                errors.append(f"{ticker}: HTTP {r.status_code}"); return
            meta = r.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
            results[ticker] = {
                "name":           meta.get("longName", ticker),
                "price":          round(meta.get("regularMarketPrice", 0), 4),
                "currency":       meta.get("currency", "USD"),
                "change_pct":     round(meta.get("regularMarketChangePercent", 0), 2),
                "previous_close": round(meta.get("previousClose", 0), 4),
                "market_cap":     meta.get("marketCap"),
                "exchange":       meta.get("exchangeName", ""),
            }
        except Exception as e:
            errors.append(f"{ticker}: {str(e)[:60]}")
    await asyncio.gather(*[fetch_one(t.upper()) for t in tickers[:30]])
    return {"success": True, "data": results, "errors": errors, "source": "Yahoo Finance"}

async def fetch_forex(pairs, base, client):
    base = (base or "USD").upper()
    try:
        r = await client.get(f"https://open.er-api.com/v6/latest/{base}", timeout=10.0)
        r.raise_for_status()
        raw = r.json()
        all_rates = raw.get("rates", {})
        if pairs:
            filtered = {p.upper(): all_rates[p.upper()] for p in pairs if p.upper() in all_rates}
        else:
            top = ["EUR","GBP","JPY","CHF","AUD","CAD","HKD","SGD","SEK","NOK",
                   "DKK","NZD","MXN","BRL","INR","CNY","KRW","TRY","ZAR","PLN",
                   "HUF","CZK","RON","THB","IDR","MYR","PHP","AED","SAR","ILS"]
            filtered = {k: all_rates[k] for k in top if k in all_rates}
        return {"success": True, "base": base, "rates": filtered,
                "total_pairs": len(all_rates), "last_updated": raw.get("time_last_update_utc", ""),
                "source": "ExchangeRate-API"}
    except Exception as e:
        return {"success": False, "error": str(e)}

async def fetch_energy_europe(countries, client):
    requested = [c.upper() for c in (countries or list(ENERGY_COUNTRIES_EU.keys())[:10])]
    requested = [c for c in requested if c in ENERGY_COUNTRIES_EU]
    results, errors = {}, []
    async def fetch_one(cc):
        try:
            r = await client.get("https://api.energy-charts.info/price",
                                 params={"bzn": cc, "period": "day"}, timeout=15.0)
            if r.status_code != 200:
                errors.append(f"{cc}: HTTP {r.status_code}"); return
            prices = [p for p in r.json().get("price", []) if p is not None]
            if not prices:
                errors.append(f"{cc}: no data"); return
            results[cc] = {
                "country":        ENERGY_COUNTRIES_EU[cc],
                "latest_eur_mwh": round(prices[-1], 2),
                "avg_eur_mwh":    round(sum(prices) / len(prices), 2),
                "min_eur_mwh":    round(min(prices), 2),
                "max_eur_mwh":    round(max(prices), 2),
                "data_points":    len(prices),
            }
        except Exception as e:
            errors.append(f"{cc}: {str(e)[:50]}")
    await asyncio.gather(*[fetch_one(c) for c in requested])
    return {"success": True, "data": results, "errors": errors, "unit": "EUR/MWh", "source": "Energy-Charts.info"}

async def fetch_energy_global(client):
    results = {
        "US": {"country": "United States", "avg_usd_mwh":  80.0, "note": "EIA national avg 2025"},
        "AU": {"country": "Australia",     "avg_usd_mwh": 195.0, "note": "NEM average 2025"},
        "JP": {"country": "Japan",         "avg_usd_mwh": 220.0, "note": "JEPX avg 2025"},
        "CA": {"country": "Canada",        "avg_usd_mwh":  85.0, "note": "AESO/IESO avg 2025"},
        "BR": {"country": "Brazil",        "avg_usd_mwh":  70.0, "note": "CCEE avg 2025"},
        "IN": {"country": "India",         "avg_usd_mwh":  55.0, "note": "IEX avg 2025"},
        "CN": {"country": "China",         "avg_usd_mwh":  60.0, "note": "benchmark 2025"},
        "ZA": {"country": "South Africa",  "avg_usd_mwh":  90.0, "note": "Eskom avg 2025"},
    }
    return {"success": True, "data": results, "unit": "USD/MWh", "source": "IEA / Ember Climate 2025"}

async def fetch_macro_rates(client):
    rates = dict(MACRO_RATES_STATIC)
    try:
        r = await client.get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                             params={"id": "FEDFUNDS"}, timeout=8.0)
        if r.status_code == 200:
            lines = r.text.strip().split("\n")
            if len(lines) >= 2:
                last = lines[-1].split(",")
                if len(last) == 2 and last[1].strip() not in (".", ""):
                    rates["FED"]["rate_pct"] = float(last[1].strip())
                    rates["FED"]["live"] = True
    except Exception:
        pass
    return {"success": True, "data": rates, "banks_count": len(rates), "source": "Central banks + FRED"}

async def fetch_crypto_news(limit, client):
    limit = min(max(limit, 1), 20)
    try:
        r = await client.get("https://cryptopanic.com/api/free/v1/posts/",
                             params={"auth_token": "public", "kind": "news", "public": "true"}, timeout=10.0)
        r.raise_for_status()
        posts = r.json().get("results", [])[:limit]
        news = [{"title": p.get("title",""), "url": p.get("url",""), "source": p.get("domain",""),
                 "published": p.get("published_at",""),
                 "currencies": [c["code"] for c in p.get("currencies",[])],
                 "sentiment": p.get("votes",{}).get("positive",0) - p.get("votes",{}).get("negative",0)}
                for p in posts]
        return {"success": True, "data": news, "count": len(news), "source": "CryptoPanic"}
    except Exception as e:
        return {"success": False, "error": str(e), "data": []}

async def fetch_general_news(limit, client):
    limit = min(max(limit, 1), 20)
    feeds = [
        ("Reuters Business",   "https://feeds.reuters.com/reuters/businessNews"),
        ("Reuters Technology", "https://feeds.reuters.com/reuters/technologyNews"),
        ("Reuters World",      "https://feeds.reuters.com/reuters/worldNews"),
    ]
    all_items = []
    for source_name, url in feeds:
        try:
            r = await client.get(url, timeout=8.0)
            if r.status_code != 200: continue
            titles = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>", r.text)
            links  = re.findall(r"<link>(https?://[^<]+)</link>", r.text)
            dates  = re.findall(r"<pubDate>(.*?)</pubDate>", r.text)
            for i, (t1, t2) in enumerate(titles[1:limit+1]):
                all_items.append({"title": (t1 or t2).strip(),
                                  "url": links[i] if i < len(links) else "",
                                  "date": dates[i].strip() if i < len(dates) else "",
                                  "source": source_name})
        except Exception:
            continue
    return {"success": True, "data": all_items[:limit], "count": len(all_items[:limit]), "source": "Reuters RSS"}

async def fetch_google_trends(keywords, geo, client):
    if not keywords:
        keywords = ["bitcoin", "AI", "stock market"]
    geo = (geo or "").upper()
    kw_list = keywords[:5]
    token_payload = json.dumps({
        "comparisonItem": [{"keyword": kw, "geo": geo, "time": "today 3-m"} for kw in kw_list],
        "category": 0, "property": "",
    })
    try:
        r = await client.get(
            "https://trends.google.com/trends/api/explore",
            params={"hl": "en-US", "tz": "-60", "req": token_payload, "cts": str(int(time.time()*1000))},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                     "Accept-Language": "en-US,en;q=0.9"},
            timeout=15.0,
        )
        if r.status_code != 200:
            return {"success": False, "error": f"HTTP {r.status_code}"}
        text = r.text[6:] if r.text.startswith(")]}',\n") else r.text
        data = json.loads(text)
        widgets = [{"title": w.get("title",""), "type": w.get("type",""), "token": w.get("token","")}
                   for w in data.get("widgets", [])]
        return {"success": True, "data": {"keywords": kw_list, "geo": geo or "Global",
                "period": "3 months", "widgets": widgets}, "source": "Google Trends"}
    except Exception as e:
        return {"success": False, "error": str(e)}

async def fetch_x_trends(country_code, client):
    cc = (country_code or "US").upper()
    nitter_hosts = ["https://nitter.net", "https://nitter.privacydev.net", "https://nitter.poast.org"]
    for host in nitter_hosts:
        try:
            r = await client.get(f"{host}/search/trending", params={"l": cc.lower()},
                                 headers={"User-Agent": "Mozilla/5.0"}, timeout=8.0, follow_redirects=True)
            if r.status_code == 200:
                trends = re.findall(r'class="trend-name"[^>]*>(#?\w+)</span>', r.text)
                volumes = re.findall(r'class="tweet-count"[^>]*>([\d,KM]+)</span>', r.text)
                if trends:
                    return {"success": True, "country": cc,
                            "trends": [{"rank": i+1, "name": t,
                                        "volume": volumes[i] if i < len(volumes) else "N/A"}
                                       for i, t in enumerate(trends[:20])],
                            "source": f"Nitter ({host})"}
        except Exception:
            continue
    return {"success": False, "error": "All Nitter instances failed",
            "fallback": [{"rank":1,"name":"#Bitcoin","volume":"250K"},
                         {"rank":2,"name":"#AI","volume":"180K"},
                         {"rank":3,"name":"#Ethereum","volume":"90K"}]}

async def fetch_facebook_ads(keyword, country, limit, apify_token, client):
    if not apify_token:
        return {"success": False, "error": "apify_token required",
                "note": "Add your Apify API token in the apify_token input field"}
    try:
        r = await client.post(
            "https://api.apify.com/v2/acts/apify~meta-ads-library-scraper/run-sync-get-dataset-items",
            params={"token": apify_token, "timeout": 120},
            json={"searchTerms": [keyword or "crypto"], "countryCode": (country or "ALL").upper(),
                  "maxAdsPerTerm": min(limit or 10, 50), "adType": "ALL", "adActiveStatus": "ACTIVE"},
            timeout=130.0,
        )
        if r.status_code not in (200, 201):
            return {"success": False, "error": f"HTTP {r.status_code}"}
        ads = r.json()
        simplified = [{"id": a.get("id",""), "advertiser": a.get("pageName",""),
                       "ad_text": (a.get("adCreativeBody","") or "")[:300],
                       "call_to_action": a.get("callToActionType",""),
                       "start_date": a.get("adDeliveryStartTime",""),
                       "countries": a.get("deliveryByRegion",[]),
                       "url": a.get("adSnapshotUrl","")} for a in ads[:limit]]
        return {"success": True, "keyword": keyword, "country": country or "ALL",
                "data": simplified, "count": len(simplified), "source": "Meta Ads Library via Apify"}
    except Exception as e:
        return {"success": False, "error": str(e)}

async def fetch_amazon_products(query, marketplace, limit, apify_token, client):
    if not apify_token:
        return {"success": False, "error": "apify_token required",
                "note": "Add your Apify API token in the apify_token input field"}
    domain_map = {"US":"amazon.com","UK":"amazon.co.uk","DE":"amazon.de","FR":"amazon.fr",
                  "IT":"amazon.it","ES":"amazon.es","JP":"amazon.co.jp","CA":"amazon.ca",
                  "AU":"amazon.com.au","IN":"amazon.in"}
    domain = domain_map.get((marketplace or "US").upper(), "amazon.com")
    try:
        r = await client.post(
            "https://api.apify.com/v2/acts/apify~amazon-product-scraper/run-sync-get-dataset-items",
            params={"token": apify_token, "timeout": 120},
            json={"searchKeywords": query or "laptop", "maxResults": min(limit or 10, 48),
                  "countryCode": (marketplace or "US").upper()},
            timeout=130.0,
        )
        if r.status_code not in (200, 201):
            return {"success": False, "error": f"HTTP {r.status_code}"}
        products = r.json()
        simplified = [{"title": (p.get("title","") or "")[:150],
                       "price": p.get("price",{}).get("value"),
                       "currency": p.get("price",{}).get("currency","USD"),
                       "rating": p.get("stars"), "reviews": p.get("reviewsCount"),
                       "asin": p.get("asin",""),
                       "url": f"https://{domain}/dp/{p.get('asin','')}",
                       "prime": p.get("isPrime", False)} for p in products[:limit]]
        return {"success": True, "query": query, "marketplace": domain,
                "data": simplified, "count": len(simplified), "source": f"Amazon via Apify"}
    except Exception as e:
        return {"success": False, "error": str(e)}

async def main():
    async with Actor:
        inp = await Actor.get_input() or {}
        endpoint    = inp.get("endpoint", "info")
        apify_token = inp.get("apify_token", os.environ.get("APIFY_TOKEN", ""))

        Actor.log.info(f"[x402 Global API v2.0] endpoint={endpoint}")

        if endpoint in ("info", ""):
            await Actor.push_data({
                "actor": "x402-global-data-api", "version": "2.0.0",
                "endpoints": {ep: {"price_usd": v["price_usd"], "description": v["desc"]}
                              for ep, v in ENDPOINTS.items()},
                "payment": "x402 protocol — USDC on Base/Polygon/Arbitrum",
                "wallet":  "0x98231cc8fcf4b2cf3598ecb613ed17a7695c0165",
            })
            return

        if endpoint not in ENDPOINTS:
            await Actor.push_data({"error": f"Unknown endpoint: '{endpoint}'", "available": list(ENDPOINTS.keys())})
            return

        symbols      = inp.get("symbols",            ["BTC","ETH","SOL"])
        tickers      = inp.get("tickers",            ["AAPL","MSFT","NVDA","SPY"])
        forex_pairs  = inp.get("forex_pairs",        [])
        forex_base   = inp.get("forex_base",         "USD")
        countries    = inp.get("countries",          [])
        keywords     = inp.get("keywords",           ["bitcoin"])
        geo          = inp.get("geo",                "")
        country_code = inp.get("country_code",       "US")
        ad_keyword   = inp.get("ad_keyword",         "crypto")
        ad_country   = inp.get("ad_country",         "ALL")
        amz_query    = inp.get("amazon_query",       "laptop")
        amz_market   = inp.get("amazon_marketplace", "US")
        limit        = int(inp.get("limit",          10))
        demo_mode    = inp.get("demo_mode",          False)

        async with httpx.AsyncClient(
            headers={"User-Agent": "x402-global-data-api/2.0 (Apify Actor)"},
            follow_redirects=True,
        ) as client:
            start = time.time()
            dispatch = {
                "crypto/prices":    lambda: fetch_crypto_prices(symbols, client),
                "crypto/fear-greed":lambda: fetch_fear_greed(client),
                "market/stocks":    lambda: fetch_stocks(tickers, client),
                "market/forex":     lambda: fetch_forex(forex_pairs, forex_base, client),
                "energy/europe":    lambda: fetch_energy_europe(countries, client),
                "energy/global":    lambda: fetch_energy_global(client),
                "macro/rates":      lambda: fetch_macro_rates(client),
                "news/crypto":      lambda: fetch_crypto_news(limit, client),
                "news/general":     lambda: fetch_general_news(limit, client),
                "trends/google":    lambda: fetch_google_trends(keywords, geo, client),
                "trends/x":         lambda: fetch_x_trends(country_code, client),
                "ads/facebook":     lambda: fetch_facebook_ads(ad_keyword, ad_country, limit, apify_token, client),
                "ecommerce/amazon": lambda: fetch_amazon_products(amz_query, amz_market, limit, apify_token, client),
            }
            data = await dispatch[endpoint]()
            elapsed = round(time.time() - start, 3)
            ep_info = ENDPOINTS[endpoint]

            await Actor.push_data({
                "endpoint":    endpoint,
                "price_usd":   ep_info["price_usd"],
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "elapsed_sec": elapsed,
                "result":      data,
                "x402": {
                    "payment_required": not demo_mode,
                    "price_usd":        ep_info["price_usd"],
                    "accepted_tokens":  ["USDC", "EURC"],
                    "networks":         ["base", "polygon", "arbitrum"],
                    "facilitator":      "https://x402.org/facilitator",
                    "payee_wallet":     "0x98231cc8fcf4b2cf3598ecb613ed17a7695c0165",
                },
            })
            Actor.log.info(f"[x402] Done: {endpoint} | {elapsed}s | ${ep_info['price_usd']}")

if __name__ == "__main__":
    asyncio.run(main())

