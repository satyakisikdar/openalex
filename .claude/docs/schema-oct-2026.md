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
