# Optimized schema, oct-2026: institutions, sources, and topic hierarchy

**Status: approved and built 2026-10-29** (31 tables, 23,789,643 rows, 101 MB in `oct-2026/`) (Decisions 1 to 4, 6, 8, and 9 as recommended; `legacy_keywords` dropped).
The code is the source of truth: `SCHEMAS` in `preprocessing/optimize_parquet.py`. Column names below use the
standardized names: `num_works` -> `num_works`, `num_citations` -> `num_citations`, `num_oa_works` ->
`num_oa_works`, and per-topic `count` -> `num_works`. Every date column is `date32` (time of day dropped).
Source: raw OpenAlex Parquet snapshot dated 2026-10-14. Ranges below are observed over every row.

## Conventions used by every table
- IDs keep only the numeric part. Types: institution, source, publisher, and funder IDs `int64`; `topic_id`
  and `subfield_id` `uint16`; `field_id` and `domain_id` `uint8` (observed values like `T14327`,
  `subfields/3312`, `fields/33`, `domains/4`; the checked cast confirms the fit at build time).
- Counts `uint32` unless noted. Years `uint16`. Dates `date32` (raw timestamps keep only the date).
- `dict` = dictionary-encoded string (pandas `category`, DuckDB `VARCHAR`).
- Child tables are keyed by the parent ID, one row per list element, and carry no parent name.
- Every file is sorted by its key columns.

## Institutions

### `institutions` (143,455 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| institution_id | int64 | id | 143,455 distinct | |
| institution_name | string | display_name | max 196 chars | |
| ror | string | ror | 1 null | prefix `https://ror.org/` stripped |
| country_code | dict | country_code | 231 values, 6,362 null | |
| type | dict | type | 9 values | |
| is_super_system | bool | is_super_system | 64 true | new |
| status | dict | status | 3 values | new |
| homepage_url | string | homepage_url | 4,726 null | |
| image_url | string | image_url | 10,770 non-null | new |
| num_works | uint32 | num_works | 0 to 42,682,843 | |
| num_citations | uint32 | num_citations | 0 to 192,331,517 | |
| 2yr_mean_citedness | float32 | summary_stats | 0 to 2,215.8 | new |
| h_index | uint16 | summary_stats | 0 to 3,605 | new |
| i10_index | uint32 | summary_stats | 0 to 2,623,490 | new |
| updated_date | date32 | updated_date | | |
| created_date | date32 | created_date | | new |

### `institutions_ids` (old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| institution_id | int64 | id | | |
| grid | string | ids.grid | 40,969 null | 1 value had a URL prefix, stripped |
| wikipedia | string | ids.wikipedia | 57,986 empty strings, stored null | `lang:Title`, e.g. `en:Women%27s_Studio_Workshop` |
| wikidata | uint32 | ids.wikidata | 26,240 non-null | Q-number; raw mixes full URLs and bare `Q123` |

### `institutions_geo` (old table)
| Column | Type | Raw source | Observed |
|---|---|---|---|
| institution_id | int64 | id | |
| city | string | geo.city | 18,128 values |
| geonames_city_id | uint32 | geo.geonames_city_id | digits only, max 13,631,347 |
| region | string | geo.region | 64,365 null |
| country | dict | geo.country | 244 values |
| latitude | float32 | geo.latitude | raw values are float32-exact |
| longitude | float32 | geo.longitude | |

### Child tables
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `institutions_associated_institutions` (old) | institution_id int64, associated_institution_id int64, relationship dict (5) | 74,210 | all IDs resolve to `institutions` |
| `institutions_counts_by_year` (old) | institution_id int64, year uint16, num_works uint32, num_oa_works uint32, num_citations uint32 | 1,355,005 | see Decision 1 (bad years) |
| `institutions_display_name_acronyms` | institution_id int64, display_name_acronym string | 63,915 | typo fixed |
| `institutions_display_name_alternatives` | institution_id int64, display_name_alternative string | 275,242 | |
| `institutions_lineage` | institution_id int64, ancestor_institution_id int64 | 39,573 | self entry dropped (always present) |
| `institutions_roles` | institution_id int64, role dict (funder, publisher), role_id int64, num_works uint32 | 18,743 | self `institution` row dropped (always equals `num_works`) |
| `institutions_repositories` | institution_id int64, source_id int64 | 3,641 | all IDs resolve to `sources` |
| `institutions_topics` | institution_id int64, topic_id uint16, num_works uint32, score float32 | 2,628,985 | |
| `institutions_topic_share` | institution_id int64, topic_id uint16, value float32 | 2,628,985 | topic sets differ from `topics` in 62% of rows, so separate |

## Sources

### `sources` (268,629 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| source_id | int64 | id | | |
| source_name | string | display_name | max 737 chars | |
| type | dict | type | 7 values | |
| issn_l | string | issn_l | 91,678 null | |
| host_organization | int64 | host_organization | 170,887 null | |
| host_organization_type | dict | letter of host_organization | publisher 80,344, institution 17,398 | new, see Decision 8 |
| host_organization_name | string | host_organization_name | | |
| num_works | uint32 | num_works | 0 to 42,704,499 | |
| num_oa_works | uint32 | num_oa_works | | |
| num_citations | uint32 | num_citations | 0 to 44,806,220 | |
| 2yr_mean_citedness | float32 | summary_stats | 0 to 1,566 | |
| h_index | uint16 | summary_stats | 0 to 1,839 | new |
| i10_index | uint32 | summary_stats | 0 to 932,403 | new |
| country_code | dict | country_code | 277 values (includes MARC codes like `ENK`, `NYU`) | |
| is_oa, is_in_doaj, is_core | bool | same | | |
| is_in_doaj_since_year | uint16 | same | 1881 to 2026 | new |
| is_high_oa_rate | bool | same | | new |
| is_high_oa_rate_since_year | int32 | same | 1828 to 2027 | new; int32 per user |
| is_in_scielo, is_ojs, is_preprint_repository | bool | same | | new |
| oa_flip_year | uint16 | same | 1827 to 2026 | new |
| first_publication_year | uint16 | same | 0 to 2027 (values 0, 1, 2, 202 exist) | new |
| last_publication_year | uint16 | same | 1384 to 2026 | new |
| apc_usd | uint16 | apc_usd | 0 to 12,690 | new; not derivable (matches latest `apc_usd_by_year` in 12,180 of 12,411) |
| homepage_url | string | homepage_url | | |
| wikidata | uint32 | ids.wikidata | 74,488 non-null | folded in from old `sources_ids`, see Decision 3 |
| updated_date | date32 | updated_date | | |
| created_date | date32 | created_date | 17,168 null | new |

### Child tables
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `sources_counts_by_year` (old) | source_id int64, year uint16, num_works uint32, num_citations uint32, num_oa_works uint32 | 3,502,191 | Decision 1 |
| `sources_issn` | source_id int64, issn string | 269,467 | |
| `sources_host_organization_lineage` | source_id int64, lineage_id int64, lineage_type dict | 17,011 | `host_organization` itself dropped (always present) |
| `sources_listed_in` | source_id int64, listed_in dict (61) | 256,561 | |
| `sources_apc_prices` | source_id int64, price uint32, currency dict (43) | 35,732 | price up to 100,000,000 (IDR) |
| `sources_apc_usd_by_year` | source_id int64, year uint16, price uint16 | 69,279 | |
| `sources_societies` | source_id int64, url string, organization string | 4,414 | |
| `sources_alternate_titles` | source_id int64, alternate_title string | 124,108 | |
| `sources_topics` | source_id int64, topic_id uint16, num_works uint32 | 5,809,836 | no score in raw |
| `sources_topic_share` | source_id int64, topic_id uint16, value float32 | 5,809,836 | |

## Topic hierarchy

### `topics` (4,516 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| topic_id | uint16 | id | | |
| topic_name | string | display_name | | |
| description | string | description | max 828 chars | |
| num_works | uint32 | num_works | 0 to 39,120,676 | old name kept, see Decision 7 |
| num_citations | uint32 | num_citations | 0 to 9,223,797 | |
| subfield_id | uint16 | subfield.id | | |
| field_id | uint8 | field.id | | old column; derivable via `subfields` but kept |
| domain_id | uint8 | domain.id | | same |
| wikipedia | string | ids.wikipedia | | `lang:Title` (wikidata is null for every topic) |
| updated_date, created_date | date32 | same | | |

Old `subfield_name`, `field_name`, and `domain_name` are dropped (join on the ID), see Decision 4.

### `subfields` (252), `fields` (26), `domains` (4); new tables, same shape
| Column | Type | Note |
|---|---|---|
| subfield_id / field_id / domain_id | uint16 / uint8 / uint8 | |
| subfield_name / field_name / domain_name | string | from display_name |
| description | string | |
| field_id, domain_id (subfields); domain_id (fields) | uint8 | parent links |
| num_works | uint32 | max 159,164,265 (domains) |
| num_citations | uint32 | max 1,138,281,592 (domains; 3.8x headroom) |
| wikidata | uint32 | raw mixes `/entity/` and `/wiki/` URLs |
| wikipedia | string | `lang:Title` |
| updated_date, created_date | date32 | |

### Child tables
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `topics_keywords` (old) | topic_id uint16, keyword_id string, keyword string, score float32 | 101,671 | `keyword_id` is the slug; not derivable from the name in 6,997 cases |
| `subfields_display_name_alternatives` | subfield_id uint16, display_name_alternative string | 393 | |
| `fields_display_name_alternatives` | field_id uint8, display_name_alternative string | 60 | |
| `domains_display_name_alternatives` | domain_id uint8, display_name_alternative string | 6 | |

## Dropped, with proof
| Raw column(s) | Why |
|---|---|
| `works_api_url`, `image_thumbnail_url` | user decision |
| `ids.openalex` (all tables) | equals `id` in every row |
| institutions `ids.ror` | equals `ror` in every row |
| institutions `type_id` | equals `https://openalex.org/institution-types/` + `type` in every row |
| institutions `geo.country_code` | equals `country_code` in every row |
| sources `ids.mag` | equals the numeric source ID in every row |
| sources `ids.issn_l`, `ids.issn` | equal `issn_l` and `issn` in every row |
| topic, subfield, field, and domain `siblings` | topics: exactly the other topics in the same subfield (4,516 of 4,516); others: every other row |
| `subfields.topics`, `fields.subfields`, `domains.fields` | exact inverse of the parent link |
| `subfield`, `field`, `domain` structs inside institution and source topic lists | consistent with the `topics` table in every element |
| names inside child lists (associated institutions, repositories, and so on) | available by joining on the ID |

## Decisions for the user
1. **Bad years in `counts_by_year`.** Institutions: 1,403 of 1,355,005 entries above 2027; sources: 1,525 of 3,502,191. Values include 10000, 2554 to 2570 (possibly Thai Buddhist-era years, offset 543; unverified), and 2025002. `uint16` cannot hold 2025002. Recommended: drop entries outside 1500 to 2027 and log counts per table.
2. **Prefixes vary within a column**, so "one prefix per column" can't hold. Recommended: normalize to a canonical form (wikidata to Q-number, grid stripped, wikipedia to `lang:Title`). Rebuilding gives the canonical URL, not the original bytes. The converter logs how many values it normalized.
3. **Drop derivable data** listed above, even where the old schema had it (`topics_siblings`, `institutions_ids.openalex` and `ror`, `sources_ids`). With those gone, `sources_ids` would hold only `wikidata`, so it is folded into `sources`. Recommended: drop.
4. **No parent names in child tables** (old tables repeated `institution_name`, `topic_name`, and so on). Recommended: drop.
5. **`legacy_keywords`** is a `; `-joined string that does not match the new keywords (0 of 4,516). Keep as `topics_legacy_keywords`, or drop?
6. **Small ID types** for the topic hierarchy (`uint16`, `uint8`), used the same way in every table. Recommended: yes.
7. **`num_works`** (old name) in `topics` vs `num_works` elsewhere. Default: keep the old name.
8. **`host_organization_type`**: host IDs are publishers or institutions, so the numeric ID alone is ambiguous. Recommended: add the type column (same for `sources_host_organization_lineage`).
9. `2yr_mean_citedness` and topic scores to `float32` lose precision beyond about 7 significant digits.

---

# Authors (approved and built 2026-10-29)

Decisions 1 to 5 taken as recommended: `raw_author_names` dropped, orphan institution IDs kept, ORCIDs
uppercased (53 + 69) or nulled (1 truncated), a `concepts` table built, and DuckDB used for the build. Row counts
below are before dropping rows that reference the deleted-institution placeholder (14 affiliations, 1 last-known).

Raw: 101,066,311 authors in 517 files (50.7 GB), identical schema in every file, IDs unique. Audit logs:
`yoda:/home/ssikdar/scratch/audit_authors.log` and `audit_authors2.log`.

## `authors` (101,066,311 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| author_id | int64 | id | max 5,158,535,892 | |
| author_name | string | display_name | max 11,542 chars (junk exists) | old name kept |
| full_name | string | full_name | 82,777 null; differs from display_name in 12.9M | new |
| orcid | string | orcid | 10,945,952 non-null | prefix stripped; 53 lowercase `x` check digits uppercased, 1 truncated value stored null |
| num_works | uint32 | works_count | 0 to 2,943,909 | |
| num_citations | uint32 | cited_by_count | 0 to 1,091,534 | |
| 2yr_mean_citedness | float32 | summary_stats | 0 to 45,798 | new |
| h_index | uint16 | summary_stats | 0 to 324 | new |
| i10_index | uint16 | summary_stats | 0 to 3,162 | new |
| updated_date | date32 | updated_date | all have a time of day (dropped) | |
| created_date | date32 | created_date | 4,485,352 null | new |

## Child tables
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `authors_display_name_alternatives` | author_id int64, display_name_alternative string | 181,538,751 | |
| `authors_observed_orcids` | author_id int64, orcid string | 11,213,951 | always includes the main `orcid`; same ORCID cleanup |
| `authors_affiliations` | author_id int64, institution_id int64, year uint16 | 262,933,039 | one row per author, institution, and year; 5,330 years outside 1500 to 2027 dropped |
| `authors_last_known_institutions` | author_id int64, institution_id int64 | 52,798,942 | always a subset of affiliations |
| `authors_topics` | author_id int64, topic_id uint16, num_works uint32 | 371,827,067 | at most 5 per author |
| `authors_topic_share` | author_id int64, topic_id uint16, value float32 | 371,827,067 | topic sets differ from `authors_topics` for 35% of authors |
| `authors_concepts` (old) | author_id int64, concept_id int64, score float32, num_works uint32 | 422,710,260 | at most 5 per author; needs a `concepts` table for names and levels |
| `authors_sources` | author_id int64, source_id int64 | 498,485,945 | 1,529,935 repeated source IDs per author removed (same ID, stale copies) |
| `authors_counts_by_year` (old) | author_id int64, year uint16, num_works uint32, num_oa_works uint32, num_citations uint32 | 301,690,683 | 100,888 entries outside 1500 to 2027 dropped |

## Dropped, with proof
| Raw column(s) | Why |
|---|---|
| `works_api_url` | user decision |
| `raw_author_names` | same set as `display_name_alternatives` in all 101,066,311 rows |
| `ids.openalex`, `ids.orcid`, `ids.observed_orcids` | equal `id`, `orcid`, and `observed_orcids` in every row |
| institution details in `affiliations` and `last_known_institutions` (ror, name, country, type, lineage) | match `oct-2026/institutions` and `institutions_lineage` in every element |
| topic hierarchy inside `topics` / `topic_share` | matches `oct-2026/topics` in every element |
| source details inside `sources` | joinable from `oct-2026/sources`; the nested copies differ from it in 15.9M of 500M elements (stale) |
| concept `display_name`, `level`, `wikidata` | constant per concept ID (63,032 IDs, 63,032 variants); belongs in a `concepts` table |
| old `authors_ids` table | its scopus, twitter, wikipedia, and mag IDs are gone from the raw data; orcid moved to `authors` |
| old `authors_hints` table | `most_cited_work` is gone from the raw data |

## Decisions for the user
1. Drop `raw_author_names` (identical to `display_name_alternatives`). Recommended.
2. Orphan institution IDs: 8,078 affiliation pairs and 5,546 last-known entries point to institutions absent
   from the institutions snapshot. Recommended: keep them (they're real IDs; joins just miss).
3. ORCID cleanup: uppercase a lowercase `x` check digit (53 values), null the 1 truncated value. Recommended.
4. Build a `concepts` table (65,026 raw rows, levels 0 to 5) with authors so `authors_concepts` joins. Recommended.
5. **Implementation:** the current converter loads a whole entity into memory. Authors (50.7 GB nested
   Parquet, about 2.1 billion child rows) won't fit on Yoda (246 GB RAM, shared). Recommended: build authors
   (and later works) with capped DuckDB `COPY ... ORDER BY ... TO` queries, which spill to disk. This makes
   DuckDB a runtime dependency. Alternative: stream raw files through pyarrow, sorted within each file
   but not globally by `author_id` (slower range filters).

# Concepts (built 2026-10-29)

`concepts` (65,026 rows): concept_id int64, concept_name string, level uint8, description string, wikidata uint32,
wikipedia string (`lang:Title`, folded in from the old `concepts_ids`), image_url string, num_works uint32,
num_citations uint32, updated_date date32, created_date date32. New relative to the old table: `image_url`,
`created_date`, and `wikipedia`.

Not buildable from this snapshot: every list and nested field in raw concepts is null for all rows
(`ancestors`, `related_concepts`, `counts_by_year`, `international`, UMLS IDs, `mag`, `summary_stats`), so the
old `concepts_ancestors`, `concepts_related_concepts`, `concepts_counts_by_year`, and `concepts_ids` tables have
no data. Dropped: `works_api_url`, `image_thumbnail_url`, `ids.openalex`, `ids.wikidata` (equals `wikidata`).

---

# Funders (approved 2026-10-30)

All 6 decisions approved as recommended; funder years are kept up to 2030 (user, for long-term grants).

Raw: 41,799 funders in 89 files (11 MB), identical schema in every file, IDs unique, no deleted placeholder.
Audit logs: `yoda:/home/ssikdar/scratch/audit_funders.log` and `audit_funders2.log`.

## `funders` (41,799 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| funder_id | int64 | id | 383,216 to 8,999,503,552 | |
| funder_name | string | display_name | max 176 chars; 41,631 distinct (same-named funders exist) | old name kept |
| country_code | dict | country_code | 178 values, 191 null | |
| description | string | description | 22,267 null | |
| homepage_url | string | homepage_url | 19,396 null | |
| image_url | string | image_url | 8,264 non-null | new |
| num_works | uint32 | works_count | 0 to 4,594,495 | counts all works, including xpac (Decision 3) |
| num_citations | uint32 | cited_by_count | 0 to 118,320,188 | |
| num_awards | uint32 | awards_count | 0 to 2,765,614; 30,113 non-zero | old `grants_count`; equals the funder's row count in raw `awards` for all 30,113 |
| 2yr_mean_citedness | float32 | summary_stats | 0 to 11,934.5 | new |
| h_index | uint16 | summary_stats | 0 to 1,994; 3,126 null (all with 0 works) | |
| i10_index | uint32 | summary_stats | 0 to 2,347,574 | new |
| updated_date | date32 | updated_date | all have a time of day (dropped) | |
| created_date | date32 | created_date | 41,777 have a time of day (dropped) | new |

## `funders_ids` (41,799 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| funder_id | int64 | id | | |
| ror | string | ids.ror | 28,060 non-null, all well formed | prefix stripped; 1,429 RORs are shared by 3,925 funders (subunits carry the parent's ROR, up to 12 per ROR) |
| wikidata | uint32 | ids.wikidata | 21,672 non-null, max Q135,404,248 | Q-number; 3 raw forms (`/entity/`, `/wiki/`, 1 without `https://www.`) normalized |
| crossref | int64 | ids.crossref | 41,777 non-null, all distinct | Crossref Funder ID; 16,160 exceed `uint32` (e.g. 501100002846) |

## Child tables
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `funders_alternate_titles` (old) | funder_id int64, alternate_title string | 96,883 | 22 repeated elements within a funder removed (`len - len(list_distinct)`); 334 lists include the display name (kept) |
| `funders_roles` (old) | funder_id int64, role dict, role_id int64 | 20,257 | 17,493 institution + 2,764 publisher; at most 1 per role per funder; 13 institution IDs absent from `oct-2026/institutions` (kept) |
| `funders_counts_by_year` (old) | funder_id int64, year uint16, num_works uint32, num_oa_works uint32, num_citations uint32 | 740,147 | years 1500 to 2030 kept (user, 2026-10-30: long-term grants; 50 entries in 2028 to 2030); 387 of 740,534 entries dropped: 386 above 2030 (max 20125; 10000 x230, 2525 to 2565, 2099, 9999) and 1 below 1500 (1445); no repeated years |

## Dropped, with proof
| Raw column(s) | Why |
|---|---|
| `ids.openalex` | equals `id` in every row |
| `ids.doi` | equals `10.13039/` + `ids.crossref` in all 41,777 non-null rows (null together in 22) |
| `image_thumbnail_url` | equals `image_url` + `&width=300` in every row |
| `roles` entries with role `funder` | always the funder itself (41,799 of 41,799), and its `works_count` equals the parent's in every row |
| `roles.works_count` for institutions and publishers | that entity's own count, already in its table; the copies are stale (differ in 6,611 of 17,480 and 750 of 2,764) |
| `funder_name` in child tables | joinable on `funder_id` |
| old `funders_summary_stats` columns `oa_percent`, `2yr_works_count`, `2yr_h_index`, `2yr_i10_index` | gone from the raw data |

## Decisions for the user
1. **Fold `summary_stats` into `funders`** (old schema had a separate `funders_summary_stats` table), as done for
   institutions and authors. Recommended.
2. **Keep `funders_ids` as its own table** (old table; the rule keeps `<x>_ids`), with `doi` dropped as derivable.
   Alternative: fold `ror`, `wikidata`, and `crossref` into `funders`. Recommended: keep the table.
3. **xpac in counts:** `works_count`, `cited_by_count`, `counts_by_year`, and `summary_stats` are OpenAlex's
   counts over all works, including xpac. Counting non-xpac works ourselves gives a different `num_works` for
   20,767 of the 38,644 funders that appear on non-xpac works (up to 506,841 fewer; 376 higher). Recomputing
   only `num_works` would make it inconsistent with the other counts. Recommended: keep the raw counts and
   document that they include xpac (same as authors and institutions).
4. **Bad years** in `counts_by_year`: drop and log. Approved, with years kept up to 2030 (387 entries dropped).
5. **`role` + `role_id`**: role IDs are institutions or publishers, so the numeric ID needs the `role` column to
   be unambiguous. Recommended: keep `role` (2 values, dictionary-encoded).
6. **Awards** is a separate raw entity (17,927,707 rows, deeply nested: investigators, institutions, topics,
   parent and sub awards). All but 1 of its 30,114 funder IDs resolve to a funder. Works link to it
   (`works.awards`, 56.1M non-xpac elements per `audit_works3.log`). Recommended: audit and build `awards` next, before works.

Facts with no decision needed: 0 orphan funder IDs on non-xpac works (38,644 distinct); 3,155 funders appear on
no non-xpac work (29 of them have `works_count` > 0); `country_code` differs from the linked institution's in 199
of 17,480 role pairs (kept as raw).
`funders_ids` keeps every funder, including up to 22 rows where `ror`, `wikidata`, and `crossref` are all null.
Publisher role IDs all resolve in raw `publishers` (0 of 2,764 orphans), but there is no `oct-2026/publishers` table yet;
it needs building for that join.

---

# Awards (approved and built 2026-10-30)

All 6 decisions approved as recommended. Built: 3 tables, 31,652,339 rows, 2.5 GB; real write byte-identical to the dry run.

New entity (the old flattener had no awards table). Raw: 17,927,707 awards in 230 files (5.1 GB; descriptions
alone are 4.6 GB), identical schema in every file, IDs unique (`G42` to `G9001171187`). Audit logs:
`yoda:/home/ssikdar/scratch/audit_awards.log` (the 4 works queries in it failed on the open-file limit and were
rerun in `audit_awards2.log`), `audit_awards3.log` to `audit_awards6.log`.

## `awards` (17,927,707 rows; new table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| award_id | int64 | id | 42 to 9,001,171,187 | |
| award_name | string | display_name | 9,405,162 null after cleanup (9,405,112 raw nulls, mostly `crossref_work_funders` and `europepmc_work_funders` stubs, plus 50 empty strings); max 9,294 chars | |
| description | string | description | 5,261,112 non-null after cleanup (5,624 empty strings stored null); max 106,824 chars; equals the name in 485,231 | |
| funder_award_id | string | funder_award_id | 17,907,301 non-null, max 1,013 chars | the funder's own grant number |
| funder_id | int64 | funder.id | 15,554 null; the other 17,912,153 all resolve to `oct-2026/funders` | |
| unlinked_funder_name | string | funder.display_name | only where `funder_id` is null: 14,660 (Canada Foundation for Innovation 14,636, DataCite 19, 5 others); 894 have no funder at all | Decision 2 |
| amount | float64 | amount | 6,861,945 non-null; -103,200,000 to 373,109,113,199; 13,868 negative, 174,615 with more than 2 decimals | in `currency` |
| currency | dict | currency | 32 values; 1,249,324 with no amount, 11,716 amounts with no currency | ISO 4217 codes |
| funding_type | dict | funding_type | 72 values, 9,406,945 null | |
| funder_scheme | dict | funder_scheme | 30,063 values after cleanup (2,459 empty strings stored null), 9,817,524 raw nulls | e.g. `R01` |
| provenance | dict | provenance | 426 values, never null | source of the record, e.g. `nih_exporter` |
| start_date | date32 | start_date | 7,537,950 non-null, 1731-01-01 to 2209-06-01 | |
| end_date | date32 | end_date | 6,942,147 non-null; 4,476 outside 1500 to 2100: 3923-04-14 x2,200, 4050-04-14 x1,615, 2101 to 2209 (644), 5397-03-31 x11, 2999-12-31 x2, 9999 x2, and 0224/0226 (1 each) | Decision 4 |
| start_year | uint16 | start_year | 7,865,995 non-null, 1731 to 2027 | not derivable: 367,550 have a year but no date, 21,280 differ from the date's year (NEH 18,591; Nobel prizes give the prize year, the date is the ceremony) |
| end_year | uint16 | end_year | 7,136,368 non-null, 224 to 9999 | 207,584 year only, 214,654 differ from the date's year |
| primary_topic_id | uint16 | primary_topic.id | 1,193,153 non-null, all in `oct-2026/topics` | |
| primary_topic_score | float32 | primary_topic.score | 0.5 to 0.9999 | |
| parent_award_id | int64 | parent_awards[].id | 280,315 non-null (at most 1 parent per award), all resolve | |
| parent_relationship | dict | parent_awards[].relationship | `component` 280,312, `sub_award` 3 | |
| parent_asserted_by | dict | parent_awards[].asserted_by | `funder` 280,312, `curated` 3 | |
| landing_page_url | string | landing_page_url | 8,210,148 non-null after cleanup (28 empty strings stored null); 732 without `http(s)://` | |
| doi | string | doi | 259,241 non-null after cleanup | `https://doi.org/` (116,590) and `http://dx.doi.org/` (46,519) stripped; 21,567 empty stored null; 13,579 mixed case kept |
| num_funded_outputs | uint32 | funded_outputs_count | 0 to 461,383; 9,807,648 awards non-zero | OpenAlex's count, includes xpac works (like funders) |
| updated_date | date32 | updated_date | all have a time of day (dropped) | |
| created_date | date32 | created_date | 17,927,706 have a time of day (dropped) | |

## Child tables
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `awards_investigators` | award_id int64, role dict, given_name string, family_name string, orcid string, role_start date32, affiliation_name string, affiliation_country string, affiliation_id_type dict, affiliation_id string, affiliation_id_asserted_by dict | 7,825,071 (7,306,449 awards) | lead 7,291,006 + co_lead 112,112 + investigator 421,953. 238,996 list entries removed: copies of the award's lead or co-lead, and exact repeats within the list (51,441 raw). 1,098,632 rows have no name or ORCID (1,096,596 of them have an affiliation); kept. `affiliation_id_asserted_by`: 10 values (`kaken` 707,489, `publisher` 73,062, `fct` 31,481, ...), not derivable from the type. ORCID: Decision 3. Country is raw text (2,670 forms: `CANADA`, `CA`, `United States`). At most 1 affiliation ID (886,062): `nrid` (Japan) 707,489, ROR 158,449 (44 of all raw ROR values are malformed, e.g. a domain name), `rnsr` (France) 17,146, 360Giving 2,928 (2 type spellings), CNPJ 50; type lowercased, value kept raw. `role_start`: 591,987 non-null, 1899-12-30 to 2027-09-01 (1899-12-30, a spreadsheet zero date, occurs once; kept) |
| `awards_institutions` | award_id int64, institution_id int64 | 5,899,561 | `institution_awarded`; every element matches `oct-2026/institutions` in name, ROR, country, type, and lineage |

The award-work link is `works.awards` and will be built with works as `works_awards` (56,114,642 non-xpac pairs, 9,770,170 awards, all resolve here).

## Dropped, with proof
| Raw column(s) | Why |
|---|---|
| `works_api_url` | equals `https://api.openalex.org/works?filter=awards.id:` + the ID in every row |
| `funder.display_name`, `funder.ror_id`, `funder.doi` (linked funders) | match `oct-2026/funders` and `funders_ids` in all 17,912,153 rows |
| `topics`, `topics_full`, `primary_topic_full` | `topics` is always `[primary_topic]` (at most 1); the `_full` copies are identical in every row |
| topic name, subfield, field, and domain (IDs and names) | match `oct-2026/topics`, `subfields`, `fields`, and `domains` in all 1,193,153 |
| `institution_awarded_full`; institution name, ROR, country, type, lineage | `_full` identical in every row; details match `oct-2026/institutions` and `institutions_lineage` in all 5,899,561 elements |
| `sub_awards`, `sub_awards_full`, `sub_awards_count` | `sub_awards_full` is the exact inverse of `parent_awards` (280,315 pairs, 0 differences), and each element's name, funder ID and name, relationship, and `asserted_by` match the child award and its parent entry in all 280,315; `sub_awards` is a prefix of it capped at 100 (301 awards); the count is its length in every row |
| `parent_awards_full`; parent name, funder ID, and funder name | `_full` identical; all three match the parent award in all 280,315 |
| `funded_outputs` | capped at 100 (38,703 awards); `works.awards` holds every non-xpac pair it has except 61, plus 9,412,077 more; of its 641,536 pairs not in `works.awards`, 641,475 point to xpac or missing works |

## Decisions for the user
1. **Build with capped DuckDB** (like authors), not the in-memory pyarrow path: 17.9M nested rows with 4.6 GB
   of descriptions. Recommended.
2. **Unlinked funder names:** 14,660 awards name a funder that has no OpenAlex ID (mostly the Canada Foundation
   for Innovation). Recommended: keep the name in `unlinked_funder_name` (null whenever `funder_id` is set), so
   these awards still say who funded them.
3. **ORCID cleanup** in `awards_investigators` (159,747 non-null): URL, `http://`, and bare forms all become the
   bare form with an uppercase `X` (152,939 valid); the 6,808 invalid values (5,000 empty strings, and values
   like `r000-0000-1569-5731` or two ORCIDs joined with `/`) stored as null and logged. Recommended.
4. **Odd dates:** keep `start_date`, `end_date`, and the years as the funder reported them (4,476 end dates
   outside 1500 to 2100, end years up to 9999), or null out values outside 1500 to 2100? Recommended: keep
   (they are the source's own values, not counts we aggregate).
5. **Duplicate awards:** 65,734 (funder, `funder_award_id`) pairs appear as 2 or 3 awards (132,171 rows),
   mostly the same UKRI grant from `gateway_to_research` and `gtr_legacy` (64,446). Works link to either ID.
   Recommended: keep all (they are distinct OpenAlex IDs); note it in the README.
6. **Empty strings to null** (names, descriptions, DOIs, investigator fields: 5,206 given names, 5,000 ORCIDs,
   360,121 affiliation countries, ...). Recommended, as everywhere else.
