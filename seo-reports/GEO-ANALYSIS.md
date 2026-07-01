# GEO Analysis — www.aarogyamfin.com
Date: 2026-07-01 (re-audit after commit `8072377`)
Previous score: 58/100 (2026-07-01, earlier same day)

## GEO Readiness Score: 68/100 (+10)

| Category | Score | Weight | Change |
|---|---|---|---|
| Citability | 21/25 | Self-canonicalization fixed → FAQ content on all 13 audience/bank pages is now reachable/indexable; homepage now opens with a clean definitional sentence | +7 |
| Structural Readability | 17/20 | Unique H1/subheading per page (was shared/duplicate); FAQ Q&A format and heading hierarchy intact | +2 |
| Multi-Modal Content | 3/15 | Still zero `<img>`/video/infographics; only credit is the upload-and-analyze tool itself as an interactive element | +1 |
| Authority & Brand Signals | 12/20 | Unchanged — `sameAs` still missing from Organization schema, blog posts still lack `Article` schema/byline/dates, no external brand presence | 0 |
| Technical Accessibility | 15/20 | Unchanged — www↔apex still serves 200 on both hosts with no 301; robots.txt/SSR/llms.txt all fine | 0 |

---

## Confirmed Fixed (from the 2026-07-01 morning audit)

1. **Self-canonicalization on 13 shared-template routes** — verified live. Each of `/bank-statement-analyzer`, `-for-ca`, `-for-dsa`, `-for-nbfc`, and the 9 bank-name variants (SBI, HDFC, ICICI, Axis, Kotak, PNB, BoB, Canara, Union) now returns its own `<link rel="canonical">`, unique `<title>`, and unique `og:url` — no longer collapsing into `/bank-statement-analyzer`.
2. **Unique H1/meta description per page** — spot-checked SBI, HDFC, NBFC pages: all have distinct H1 headline and meta description tailored to the audience/bank.
3. **Homepage definitional sentence** — hero now opens with: *"AarogyamFin is an AI-powered financial workflow platform that turns bank statements, GST, and tax data into ready-to-use financials for CAs, DSAs, NBFCs, and banks."* — a clean, quotable "what is this" passage in the first \~25 words.
4. **llms.txt rewritten** — now describes the current GST/ITR/FOIR/books-finalisation positioning (previously described only "bank statement analyzer"), and links out to 20 pages including all audience/bank landing pages, the consolidator, and GST calendar.
5. **FAQPage schema confirmed present** on landing pages (`@type: FAQPage` with `Question`/`Answer` nodes), in addition to `SoftwareApplication` + `Offer` — this was previously unconfirmed.
6. **Programmatic keyword-variant pages unaffected/healthy** — spot-checked 5 of the ~15 synonym landing pages (`/bank-statement-to-excel`, `/pdf-bank-parser`, etc., which use a different template than the 13 audience/bank routes): all already self-canonicalize correctly with unique titles, so no regression there.
7. **AI crawler access confirmed** — served identical HTML to default UA, `GPTBot`, and `PerplexityBot` (only Cloudflare per-request nonce and rotating email-cloaking token differ — both harmless).

## Still Open (carried over, unaddressed by this commit)

| Issue | Detail |
|---|---|
| **www ↔ apex duplicate host** | `https://www.aarogyamfin.com/` and `https://aarogyamfin.com/` both return `200` with identical content — still no 301. Canonical tag correctly points to apex, so Google should be fine, but AI crawlers are less consistent about respecting canonical than Google is. |
| **Blog posts lack `Article` schema / byline / dates** | `/blog/how-to-analyze-bank-statement-for-itr` still has no `Article` JSON-LD, no author, no `datePublished`/`dateModified`. |
| **No `sameAs` on Organization schema** | No LinkedIn/GitHub/social links in the Organization node. |
| **No brand presence off-site** | No evidence of Wikipedia, Reddit, YouTube, or LinkedIn mentions. Given Ahrefs' correlation data (YouTube ~0.737, Reddit high) this is the highest-effort, highest-payoff category still untouched — expected, since this is a young (`foundingDate: 2025`) product. |
| **Zero images/video sitewide** | Landing pages, homepage, and blog post sampled all have 0 `<img>` tags and no video. Multi-modal content sees 156% higher selection rates per GEO research — this is the single biggest remaining scoring gap (3/15). |

---

## Top 5 Highest-Impact Changes (updated)

1. **Add `Article` schema + visible byline + `datePublished`/`dateModified`** to all blog posts — cheap, high-authority signal that's still fully missing.
2. **Implement the Cloudflare www→apex 301 redirect** — outstanding since the April 2026 audit; canonical tags mitigate but don't fully substitute for a real redirect with AI crawlers.
3. **Add at least one relevant image per landing page** (e.g. a screenshot of the analyzer output/dashboard) with descriptive alt text — moves Multi-Modal Content off its 3/15 floor with minimal effort.
4. **Add `sameAs` array to Organization schema** once any social profiles exist (LinkedIn company page is the easiest first step for a B2B fintech).
5. **Begin off-site brand-mention work** (Reddit threads in r/IndiaInvestments/r/CharteredAccountants, a YouTube demo walkthrough) — this is the category with the largest score gap (12/20) and the strongest citation correlation per Ahrefs data, but requires ongoing effort rather than a one-time fix.

## Schema Recommendations

- `Article` + `Person` (author) schema for every blog post.
- `sameAs` on the existing `Organization` node.
- Consider `HowTo` schema on the ITR blog post given its step-by-step structure — would aid both traditional rich results and AI extraction of procedural content.

## Content Reformatting Suggestions

- Blog posts: add "By [Name], Founder — Updated July 2026" byline block matching the schema author.
- Landing pages: no further content change needed — FAQ content is already audience-specific and now correctly indexable.
- Consider a short screenshot/GIF of the upload → analysis → Excel export flow embedded on the homepage and top landing pages (SBI, HDFC, CA) to address the multi-modal gap.
