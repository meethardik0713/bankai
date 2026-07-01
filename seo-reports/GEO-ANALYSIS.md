# GEO Analysis — www.aarogyamfin.com
Date: 2026-07-01

## GEO Readiness Score: 58/100

| Category | Score | Weight |
|---|---|---|
| Citability | 14/25 | FAQ passages are excellent, but they live on pages that are self-canonicalized away (see Critical Issue) |
| Structural Readability | 15/20 | Clean H1→H2→H3 hierarchy on homepage; FAQs are proper Q&A format on landing pages |
| Multi-Modal Content | 2/15 | Zero `<img>` tags, no video, no infographics found on homepage or sampled landing/blog pages |
| Authority & Brand Signals | 12/20 | Org/SoftwareApplication schema present but lost `sameAs` links; no author byline/date on blog articles |
| Technical Accessibility | 15/20 | Server-rendered HTML (no JS-dependent content), AI crawlers allowed, llms.txt present but stale |

---

## Critical Issue: 18 Landing Pages Canonicalize Away From Themselves

This is the single biggest problem found, and it directly undoes recent work. Git history shows unique FAQs were added to 18 landing pages (`5bffead SEO: unique FAQs added to all 18 landing pages`), but every audience/bank-specific variant carries a canonical tag pointing to the generic `/bank-statement-analyzer` page instead of itself:

| Page | Canonical points to |
|---|---|
| `/bank-statement-analyzer-for-ca` | `/bank-statement-analyzer` |
| `/bank-statement-analyzer-for-dsa` | `/bank-statement-analyzer` |
| `/bank-statement-analyzer-for-nbfc` | `/bank-statement-analyzer` |
| `/sbi-bank-statement-analyzer` | `/bank-statement-analyzer` |
| `/bank-statement-to-excel` | itself (correct) |

**Effect:** Google will not index these as distinct pages, and AI crawlers/LLMs treat the canonical URL as the authoritative source — meaning the CA-specific, DSA-specific, and bank-specific FAQ content (e.g. "How does loan eligibility work for NBFCs?") will likely never surface in AI Overviews, ChatGPT, or Perplexity answers keyed to those specific audience/bank queries, even though the content exists and is well-written. This is the highest-leverage fix on this list — it's a one-line template bug (self-canonical vs. hardcoded canonical), not new content work.

**Fix:** In the page template, canonical should be `request.path` (self-referencing) unless the page is a genuine near-duplicate you intend to consolidate. If some consolidation is intentional (e.g. very thin bank-name variants), keep it only for those — not for the CA/DSA/NBFC audience pages, which have materially different FAQ content.

---

## AI Crawler Access Status

robots.txt (served identically on www and non-www):

```
User-agent: *            Allow: /
User-agent: Googlebot    Allow: /
User-agent: Google-Extended  Allow: /
User-agent: GPTBot       Allow: /
User-agent: ClaudeBot    Allow: /
User-agent: Applebot-Extended  Allow: /
User-agent: meta-externalagent Allow: /
User-agent: CCBot        Disallow: /
User-agent: Bytespider   Disallow: /
```

- ✅ ClaudeBot, GPTBot, Google-Extended, Applebot-Extended explicitly allowed
- ✅ CCBot and Bytespider blocked (reasonable, avoids uncredited training scrapers)
- ⚠️ PerplexityBot, OAI-SearchBot, ChatGPT-User, anthropic-ai have no explicit rule — they fall under the wildcard `Allow: /`, so they **are** allowed, but making them explicit is cheap insurance against future default-deny changes by those crawlers.

## www vs non-www Duplicate Host (Still Unresolved)

Both `https://www.aarogyamfin.com/` and `https://aarogyamfin.com/` return `200 OK` with identical content — there is still no 301 redirect between hosts (flagged in the prior audit, April 2026, still open). The saving grace: the `<link rel="canonical">` on every page correctly points to the non-www apex domain, so search engines should consolidate signals correctly. However, AI crawlers are less consistent about respecting canonical tags than Google is, so a real Cloudflare-level 301 (www → apex) remains the more reliable fix and should still be prioritized.

## llms.txt Status: Present but Out of Date

`/llms.txt` exists and is well-formed, but describes an old version of the product:

```
AarogyamFin is an AI-powered Indian bank statement analyzer.
Users upload bank statement PDFs and instantly get transaction analysis,
keyword search, Excel export, and AI chat insights.
```

The live homepage title/meta description and JSON-LD now describe a much broader "AI CA Workflow Platform" covering **GST compliance (GSTR-1, 3B, 2B reconciliation), 26AS/AIS consolidation, books finalisation (Balance Sheet & P&L), and DSA/NBFC/CA-specific workflows** — none of which appear in llms.txt. An LLM reading llms.txt today would describe AarogyamFin as a much narrower tool than it actually is, undercutting citations for GST/CA-related queries. llms.txt should be rewritten to match current positioning and link to the new landing pages (once their canonical issue above is fixed).

## Brand Mention Analysis

No evidence found (via this audit) of presence on Wikipedia, Reddit, YouTube, or LinkedIn. This was already flagged in the April 2026 audit as an open item requiring off-site work. Given the Ahrefs correlation data (YouTube ~0.737, Reddit strong), this remains the highest-effort/highest-payoff category still untouched.

## Passage-Level Citability

- **Landing page FAQs** (sampled on `/bank-statement-analyzer-for-ca`): well-formed, self-contained, in the 30–70 word range — e.g. the loan eligibility and AI-chat answers are exactly the kind of quotable, fact-dense block AI Overviews extract. Quality is good; **reach is blocked by the canonical issue above.**
- **Homepage**: ~698 words of visible text, no FAQ section, no "X is..." definitional sentence in the first 60 words — the hero headline is a value prop, not a definition. Adding one definitional sentence near the top (e.g. "AarogyamFin is an AI-powered financial workflow platform that turns bank statements, GST, and tax data into ready-to-use financials for CAs, DSAs, NBFCs, and banks.") would give AI systems a clean, quotable "what is this" passage.
- **Blog post** (`/blog/how-to-analyze-bank-statement-for-itr`): 2,475 words, correctly self-canonicalized, but no `Article` schema and no visible author byline or published/updated date — a missed authority signal.

## Server-Side Rendering Check

No issue: homepage HTML contains ~698 words of real text without executing JavaScript, and Three.js/Chart.js are loaded for visual/decorative purposes rather than gating content. AI crawlers (which don't run JS) can read the actual page content fine.

---

## Top 5 Highest-Impact Changes

1. **Fix canonical tags on the 18 audience/bank landing pages** so each self-canonicalizes (or is intentionally, selectively consolidated). This alone determines whether the recent FAQ content work has any AI/search visibility at all.
2. **Rewrite `/llms.txt`** to reflect the current GST/CA/books-finalisation positioning, not just the original bank-statement-analyzer description.
3. **Add `Article` schema + visible author byline + published/updated dates** to blog posts for authority signal.
4. **Add one definitional sentence** ("AarogyamFin is...") near the top of the homepage for direct-answer extraction.
5. **Implement the Cloudflare www→apex 301 redirect** (still outstanding from the April audit) so AI crawlers that don't honor canonical tags don't see two live copies of the site.

## Schema Recommendations

- Add `FAQPage` schema to landing pages (visible FAQ markup exists in HTML but wasn't confirmed as schema-wrapped on `/bank-statement-analyzer-for-ca` — verify and add if missing, since visible FAQs without schema still help GEO but schema helps traditional rich-result eligibility).
- Add `Article` + `Person` (author) schema to blog posts.
- Restore `sameAs` array on the `Organization` node once social/brand profiles exist (LinkedIn, GitHub, etc.) — present in the April audit's JSON-LD rewrite but not appearing in the current homepage schema.

## Content Reformatting Suggestions

- Homepage hero: prepend a single definitional sentence before the current value-prop copy.
- Blog posts: add a byline block ("By Hardik, Founder — Updated July 2026") near the top, matching what schema will declare.
- Landing pages: once canonical is fixed, each audience page's FAQ should stay audience-specific (already true) — no content change needed there, only the canonical/indexation fix.
