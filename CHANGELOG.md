# v0.1.0-dev

* Enhancements
  * Add `ecto.migrate` and `ecto.rollback` tasks, support `--to`, `--step` and `--all`
  * Do not require Ecto URI schema to start with `ecto`
  * Allow `:on` with association joins on keywords syntax
  * Add Decimal support
  * Add 'distinct' query expression
  * Add `Validator.bin_dict/2`
  * Add `Ecto.Repo.rollback` for explicit transaction rollback
  * Add support for timeouts on Repo calls

* Bug fixes
  * Fix association functions resetting the entity when manually loading associated entities
  * Fix a bug where an association join's 'on' expression didn't use the bindings

* Deprecations

* Backwards incompatible changes
  * `Ecto.Binary[]` is no longer used to wrap binary values. Instead always use `binary/1` in queries
  * `:list` type changed name to `:array`. Need to specify inner type for arrays in entity fields
  * Literal lists no longer supported in queries. Need to specify inner type; use `array(list, ^:integer)` instead
  * Remove `url/0` for configuration of repos in favor of `conf/0` in conjunction with `parse_url/1`

# v0.0.1

* Initial release
