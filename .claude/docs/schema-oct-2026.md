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

# Works

**Status: proposed 2026-10-30; decided so far: 1, 2, 4 (part), 8, 14, 18, and 19 (marked DECIDED below).** Nothing built. Abstracts are built last (decided) and are only sketched here.

Raw: 464,765,485 works in 1,898 files (696 GB with abstracts), identical schema in every file, IDs unique (max
7,221,746,310), none in `deleted_ids.csv.gz` (71,528,591 IDs). xpac works (137,776,075) and all their child rows
are skipped, and an xpac work counts as absent everywhere (decided), leaving **326,989,410 works**. Every count below
is non-xpac. Audit logs in `yoda:/home/ssikdar/scratch/`: `audit_works.log` (all works, includes xpac),
`audit_works2.log` (lineage), `audit_works3.log` (anti-join orphans, row counts), `audit_works4.log` (no-year works,
self-citations), `audit_works5_report.md` (independent pyarrow check), `audit_works6.log` (copies vs built tables),
`audit_works6b.log` (authorship dedupe; the 6 version ran out of memory), `audit_works6c.log` (funders),
`audit_works6d.log` (location IDs, licenses), and `audit_awards2.log` (works.awards vs awards, non-xpac). Numbers marked
(all works) come from `audit_works.log` and include xpac works.

Already decided (`parquet.md`): non-xpac everywhere; `<entity>_id` names; `has_*` columns are `bool`; `num_authors`
counts distinct author IDs; `publication_year` on every table; no lineage expansion in `works_authorships`; no
`works_related_works`; drop the 4,887,757 placeholder references (W4285719527) and the 1,059,673 self-references;
keep the 3,168,890 no-year works with a null year; dedupe MeSH and repeated authors; `language` dict-encoded.

19 tables. Without `works_abstracts` (180,705,981, built last): 18 tables, 12,946,589,803 rows as proposed (Decision 4
adds about 1.4B; declining Decision 5 adds 122,945,088). Output size is measured at the
dry run; `/data` has 2.0 TB free.

## `works` (326,989,410 rows; old table)
| Column | Type | Raw source | Observed | Note |
|---|---|---|---|---|
| work_id | int64 | id | up to 7,221,746,310 (all works) | |
| doi | string | doi | 219,790,314 non-null; 4,006 bare `https://doi.org/` stored null; 111 more flagged malformed by audit5, kept | prefix stripped |
| title | string | title | 1,642,752 null; 627,446 empty after trimming, stored null; max 5,000 chars (all works); audit5's whitespace test finds 626,228 empty, the build's count is logged | `display_name` dropped (equal) |
| publication_year | uint16 | publication_year | 1007 to 2050; 3,168,890 null; 2,946 outside 1500 to 2027 | Decision 15 |
| publication_date | date32 | publication_date | null exactly when the year is; year always matches | |
| type | dict | type | 25 values (all works) | `type_crossref` gone from raw |
| language | dict | language | 22,997,308 null, 77 empty stored null; 716 raw forms (all works; `en`, `EN`, `zh-cn`, `中文`) | as is (decided) |
| num_authors | uint16 | authorships | max 4,943; 0 in 41,191,378 | distinct non-null author IDs (decided) |
| num_locations | uint16 | locations_count | max 1,196 | equals list length in every row |
| num_references | uint16 | referenced_works_count | max 5,000 | Decision 5 |
| num_citations | uint32 | cited_by_count | 0 to 801,217 | |
| num_countries | uint8 | countries_distinct_count | max 131 | **new** |
| num_institutions | uint16 | institutions_distinct_count | max 1,458 | **new** |
| fwci | float32 | fwci | 235,131,876 non-null, 0 to 16,429.6 | **new** |
| citation_percentile | float32 | citation_normalized_percentile.value | 236,193,180 non-null, 0 to 1 | **new** |
| is_in_top_1_percent, is_in_top_10_percent | bool | citation_normalized_percentile.* | | **new** |
| cited_by_percentile_year_min, cited_by_percentile_year_max | uint8 | cited_by_percentile_year | 91,349,198 non-null, 0 to 100 | **new** |
| apc_list_value, apc_list_currency, apc_list_usd | uint32, dict, uint16 | apc_list | 62,468,930 non-null; value max 100,000,000; USD max 12,690; 41 currencies; minimum not audited (raw int32, the checked cast fails loudly if negative) | **new** |
| apc_paid_value, apc_paid_currency, apc_paid_usd | uint32, dict, uint32 | apc_paid | 13,886,124 non-null; 0 fractional values; value max 100,000,000; USD max 40,261; 32 currencies | **new**; raw is double |
| volume_work_id | int64 | volume.id | 26,090,789 non-null; 59,083 not a non-xpac work | **new**, Decision 13 |
| volume_name | string | volume.display_name | differs from the volume work's title in 291,956 of 26,031,706 | **new**, Decision 13 |
| has_complete_institution_info | bool | derived | 121,017,413 true | old meaning: at least one authorship with an author ID, and each of those has an institution ID |
| has_grant_info | bool | `awards` non-empty | | old meaning, now from `awards` |
| has_keywords | bool | `keywords` non-empty | | |
| has_fulltext | bool | has_fulltext | 56,067,907 true | |
| has_pdf, has_grobid_xml | bool | has_content.* | 55,351,470 and 53,071,376 true | **new** |
| is_retracted, is_paratext | bool | same | 136,811 and 6,352,659 true | |
| updated_date | date32 | updated_date | time of day dropped | |
| created_date | date32 | created_date | 0 null | |

## Child tables (every one has `work_id int64` and `publication_year uint16`, sorted by key)
| Table | Columns (type) | Rows | Note |
|---|---|---|---|
| `works_ids` (old) | mag int64, pmid int64, openalex_curation string | 326,989,410 (1 per work) | 190,001,736 MAG, 40,941,838 PMID (0 malformed), 2,265 curation IDs (**new**, e.g. `oxjob747-...`). `pmcid` is gone from raw |
| `works_open_access` (old) | is_oa bool, oa_status dict (6), oa_url string, any_repository_has_fulltext bool | 326,989,410 | oa_url 127,446,679 non-null (176,022 contain spaces, kept); 126 open works have no URL |
| `works_biblio` (old) | volume, issue, first_page, last_page string | 326,989,410 | gains `publication_year`; 206 empty volumes and 5,615 empty first pages stored null |
| `works_authorships` (old) | author_order uint16, author_position dict (3), author_id int64, raw_author_name string, raw_orcid string, is_corresponding bool, institution_id int64, raw_affiliation_string string | 945,402,701 | one row per kept authorship copy and institution (null when it has none), `audit_works7.log`. Dropped: the 64,718,630 authorships with no author ID, 774,000 copies of 655,353 orphan author IDs, and repeated copies of an author with the same institution set (813,332,439 non-orphan copies kept as 812,977,432 work-author pairs plus 355,007 copies with a different set, in 339,625 pairs; such an author can repeat an institution under two `author_order` values). 285,379,865 works keep at least one authorship. 9,451 rows name 123 institutions not in `oct-2026/institutions` (kept, pending your answer). `raw_affiliation_string`: the copy's strings joined by `;`. Matches audit7 exactly on a one-file smoke test |
| `works_corresponding_institutions` | institution_id int64 | 129,921,067 | **new**; not derivable (differs from the corresponding authors' institutions in 32,021,718 works); 0 repeats; 1,574 dangling, kept |
| `works_locations` (old) | location_order uint16, is_primary bool, is_best_oa bool, native_id string, source_id int64, doi string, landing_page_url string, pdf_url string, version dict, license dict, is_oa bool, is_accepted bool, is_published bool, raw_source_name string, raw_type string, provenance dict | 493,204,605 | replaces `works_primary_location` and `works_best_oa_location` (Decision 6). Primary is always order 1; best OA is always in the list (127,446,652 works). source_id 441,063,741 non-null, 0 orphans. Location DOI differs from the work DOI for 6,234,659 primaries (kept). version: 3 values plus 135,233,215 null; license 10; provenance 6 (crossref, repo, mag, datacite, pubmed, curation). 76,644 landing and 171,387 PDF URLs contain spaces, kept |
| `works_topics` (old) | is_primary_topic bool, score float32, topic_id uint16, topic_name dict, subfield_id uint16, subfield_name dict, field_id uint8, field_name dict, domain_id uint8, domain_name dict | 909,866,043 | the exact may-2025 column list (your directive), types tightened; Decision 8. At most 3 per work; primary is always first; 0 orphans; every name and hierarchy ID matches `oct-2026/topics` |
| `works_keywords` (old) | keyword_id string, keyword dict, score float32 | 1,835,438,003 | 1,877,787 slugs (prefix stripped; 14,785 rows with non-ASCII slugs, valid); score 0.301 to 1 (all works). Decision 9 |
| `works_concepts` (old) | concept_id int64, score float32 | 2,983,557,411 | 153,273 repeats removed (16,654 works), Decision 10; 0 orphans |
| `works_mesh` (old) | descriptor_ui dict, descriptor_name dict, qualifier_ui dict, qualifier_name dict, is_major_topic bool | 405,160,042 | dedupe removes 888,391,044 of 1,293,551,086 raw rows (33,365,092 works), Decision 11. Gains `publication_year`; `is_major_topic` was a string in may-2025. 329 rows have a descriptor ID starting with `C` (supplementary concepts), kept |
| `works_referenced_works` (old) | work_publication_year uint16, referenced_work_id int64, referenced_work_publication_year uint16 | 2,886,600,388 | from 3,015,492,906 raw: minus 4,887,757 placeholders and 1,059,673 self-references (decided), minus 122,945,088 references to works outside the build (xpac, merged, or missing; 4,098,040 of them in `deleted_ids`), Decision 4. Both years (your directive); 0 repeats |
| `works_indexed_in` (old) | indexed_source dict (5, all works) | 283,508,753 | crossref, datacite, pubmed, doaj, arxiv |
| `works_counts_by_year` | year uint16, num_citations uint32 | 464,280,714 | **new**; years 2012 to 2026 (all works; audit5 confirms 2012 to 2027 for non-xpac), 0 repeats |
| `works_awards` | award_id int64 | 56,114,642 | **new**, replaces `works_grants`: 9,770,170 awards on 17,353,432 works, all resolve to `oct-2026/awards`, 0 repeats (`audit_awards2.log`). The award name, funder grant number, funder ID, and funder name in each element match `awards` in all 56,114,642, so only the ID is kept. The old `award_id` was the funder's grant number; that is now `awards.funder_award_id` |
| `works_funders` | funder_id int64 | 62,850,709 | **new**; 38,644 funders, 0 orphans, name and ROR match `oct-2026/funders` in every row; 0 repeats |
| `works_sustainable_development_goals` | sdg_id uint8, score float32 | 160,631,568 | **new**; 17 goals, score 0.4 to 1; name constant per goal |
| `works_study_designs` | study_design dict | 22,095,517 | **new**; 7 slugs (`observational-study`, `case-report`, ...); name constant per slug |
| `works_abstracts` (old, built last) | abstract string | 180,705,981 | plain text rebuilt from the inverted index; old `title` column dropped (in `works`); 2 invalid JSON rows in a sample of 1,566,193 |

## Dropped, with proof
| Raw column(s) | Why |
|---|---|
| `display_name` | equals `title` in every row |
| `authors_count` | dropped (user, 2026-10-30): the raw list length counts repeats and dropped authorships; `num_authors` counts the kept authors |
| `ids.openalex`, `ids.doi` | equal `id` and `doi` in every row |
| `is_xpac` | xpac works are skipped |
| top-level `institutions` | empty in every work |
| `related_works` | decided (no table) |
| `primary_location`, `best_oa_location` | primary is `locations[1]` in every work; best OA is always in `locations`: kept as `is_primary`, `is_best_oa` |
| `primary_topic` | always `topics[1]`: kept as `is_primary_topic` |
| `authorships.author.display_name`, `.orcid` | match `oct-2026/authors` in all 816,681,687 joined authorships (213 ORCIDs differ only in the case of `X`); the 783,770 orphan authorships lose them, Decision 18 |
| `authorships.author.observed_orcids` | one list per author everywhere (95,615,945 authors); equals `oct-2026/authors_observed_orcids` in all but 4,398 of 438,136,797 non-empty authorships: 3,929 are orphan authors (dropped) and the rest list one ORCID twice with `x` and `X` (`audit_works7.log`) |
| `authorships.institutions` name, ROR, country, type | match `oct-2026/institutions` in all 646,739,289 joined rows; the 9,772 orphan rows lose them, Decision 18 |
| `authorships.institutions.lineage` | matches `institutions_lineage` (0 differences over 125,777 variants) |
| `authorships.raw_affiliation_strings` | the same set as `affiliations[].raw_affiliation_string` in all 882,184,087 authorships |
| location `source` name, ISSN-L, type, host organization | match `oct-2026/sources` in all 441,063,741 |
| location `source` ISSN list, `is_in_doaj`, `is_core`, `listed_in`, host name | match `oct-2026/sources`, `sources_issn`, and `sources_listed_in` in all 441,063,741 (`audit_works7.log`) |
| location `source.is_oa` | stale copy: false in 11,701,300 locations (15,572 sources) where `sources.is_oa` is true (`audit_works7b.log`); dropped (decided) |
| location `source` host lineage and lineage names | without the host itself, equals `sources_host_organization_lineage` in all but 3,767,825 locations (1,270 sources), where the built table has more ancestors (`audit_works7b.log`); lineage names of publisher IDs can't be checked (no publishers table); dropped (decided) |
| location `license_id` | equals `https://openalex.org/licenses/` + `license` in all 493,204,605 |
| topic names and subfield, field, domain IDs and names | match `oct-2026/topics`, `subfields`, `fields`, and `domains` in all 909,866,043 (kept in `works_topics` per Decision 8) |
| concept `display_name`, `level`, `wikidata` | match `oct-2026/concepts` in all 2,983,710,684 |
| award element fields other than `id` | match `oct-2026/awards` in all 56,114,642 |
| funder `display_name`, `ror` | match `oct-2026/funders` in all 62,850,709 |
| SDG and study design `display_name` | constant per ID (17 and 7 pairs) |

## Decisions for the user
1. **DECIDED 2026-10-30: drop a repeated copy of an author only when its institution set (possibly empty) equals
   an earlier copy's; copies with a different set are kept as their own `author_order` rows.** Counts from
   `audit_works7.log`. Was: **Repeated authors** (3,714,049 extra copies of a (work, author) pair in 1,967,330 works; the copies disagree on
   institutions 355,031 times, position 1,159,371, name 1,146,255, corresponding flag 321,056, raw strings 473,387).
   Recommended: `author_order`, `author_position`, and `raw_author_name` from the first copy; `is_corresponding`
   true if any copy is; institutions and raw strings unioned. 1,017,785,385 rows.
2. **DECIDED 2026-10-30: drop them (and log the count).** Was: **Authorships with no author ID** (64,718,630; the old flattener dropped them). Recommended: keep them with a
   null `author_id` and `author_order` (list position, 1-based), so names and affiliations of unmatched authors
   survive. 71,879,138 rows.
3. **`raw_orcid`** (new; 147,793,139 non-null, 323 with a lowercase `x`). Recommended: keep, bare form with an
   uppercase `X`, as in awards.
4. **DECIDED 2026-10-30 (part): no `works_authorships_affiliations` (works_authorships keeps the work, author,
   institution tuples); countries pending `audit_works8.log`.** Was: **Authorship countries and affiliations.** `countries` differs from the institutions' countries in 1,664,425
   authorships, always where an institution has no country code; and `affiliations` is the only link from each raw
   string to its institutions (153,062,999 authorships have 2 or more; the institution sets differ in 131,289).
   Recommended: add `works_authorships_countries` (author_order, country_code; 519,530,884 rows before dedupe) and
   `works_authorships_affiliations` (author_order, raw_affiliation_string, institution_id; 887,844,526 rows before
   dedupe). Declining both loses this information.
5. **References to works outside the build** (122,945,088: xpac, merged, or missing). Recommended: drop them, as the
   old flattener dropped deleted and merged targets and "everything non-xpac" makes xpac targets absent; then every
   `referenced_work_id` joins `works`. `num_references` keeps OpenAlex's raw count (like `num_funded_outputs`), so it
   can exceed the table's rows. Keeping them instead: 3,009,545,476 rows, null target year.
6. **One `works_locations` table** with `is_primary` and `is_best_oa` instead of three old tables. Also keep the raw
   location `id` as `native_id` (a source-side ID such as a DOI, not a W ID; 491,611,736 distinct of 493,204,605;
   not derivable). Recommended.
7. **1:1 old tables** `works_ids`, `works_open_access`, `works_biblio` stay separate, one row per work; the new 1:1
   fields go in `works`. Recommended.
8. **DECIDED 2026-10-30: keep topic, subfield, field, and domain names.** Was: **`works_topics` with names** (your directive to match the existing table; dict-encoded, so cheap) versus IDs
   only (the convention elsewhere). Recommended: with names, as you asked.
9. **Keyword names** are not constant per ID in 22 slugs (`massachusetts` is both "Massachusetts" and "mechanical
   alloying"). Recommended: keep `keyword` (the old column) next to `keyword_id`, dict-encoded; no lookup table.
10. **Repeated concepts** (16,654 works; 144,685 exact copies plus 8,588 with a different score). Recommended: keep
    the first occurrence of each concept. 2,983,557,411 rows.
11. **MeSH dedupe key** (work, descriptor_ui, qualifier_ui); `is_major_topic` true if any copy is; names from the
    first copy. Descriptor names vary in 77 IDs (2,242,666 rows; MeSH renames such as "Major Depressive Disorder"
    and "Depressive Disorder, Major"), so names stay in the table. 405,160,042 rows (exact copies only would leave
    406,285,705). Recommended.
12. **New columns** marked **new** above need your approval (CLAUDE.md rule). Recommended: all.
13. **Volume:** keep `volume_work_id` including the 59,083 that point outside the build, and keep `volume_name`
    (not derivable). Recommended.
14. **DECIDED 2026-10-30: drop.** Was: **`observed_orcids`** (a list per authorship author, not audited). Recommended: drop; the ORCID lives on the
    author profile. Say if you want it audited first.
15. **Years outside 1500 to 2027** (2,946 values, min 1007, max 2050): keep as is, and log. Recommended.
16. **Empty strings to null** (627,446 titles, 4,006 DOIs, 77 languages, 206 volumes, 5,615 first pages, 1,542,774
    affiliation strings). Recommended, as everywhere else.
17. **Build with capped DuckDB**, one statement per table, dedupe done per work with list functions (a global
    group-by over the 817M authorships ran out of memory at 60 GB in `audit_works6`). Recommended.
18. **DECIDED 2026-10-30: drop authorships whose author ID is not in `oct-2026/authors`.** Was: **Orphans lose their nested details.** The 783,770 authorships whose author ID is not in `oct-2026/authors` lose
    the author's name and ORCID copy, and the 9,772 rows with an institution not in `oct-2026/institutions` lose
    its name, ROR, country, and type. Recommended: accept (IDs and raw strings are kept; under 0.1%).
19. **DECIDED 2026-10-30: drop them** (audited anyway in `audit_works7.log`). Was: **Location source fields not compared** (ISSN list, OA, DOAJ, core flags, `listed_in`, host name and lineage).
    Recommended: drop; they describe the source and live in the `sources` tables. Say if you want a check first.
20. **`corresponding_author_ids`** matches the `is_corresponding` authorships in all but 1 work (W4283721846).
    Recommended: drop the column and log that work.
21. **`raw_orcid` cleanup (Decision 3)** assumes the 323 malformed values are a lowercase `x`; the build logs any
    value that still fails the ORCID pattern and stores it null.
