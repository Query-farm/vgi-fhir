# Copyright 2026 Query Farm LLC - https://query.farm
#
# Rewrite each `require <ext>` gate in this repo's sqllogictest files into an
# explicit signed INSTALL+LOAD, so the prebuilt standalone `haybarn-unittest`
# (which links none of these extensions) can run the suite. The vgi extension
# comes from the signed community channel; httpfs/json/parquet/spatial from the
# signed core channel. `require-env` and every other directive pass through
# untouched. See ci/README.md.
/^require[ \t]+vgi[ \t]*$/ {
    print "statement ok"; print "INSTALL vgi FROM community;"; print "";
    print "statement ok"; print "LOAD vgi;"; next
}
/^require[ \t]+(httpfs|json|parquet|spatial)[ \t]*$/ {
    ext = $2
    print "statement ok"; print "INSTALL " ext " FROM core;"; print "";
    print "statement ok"; print "LOAD " ext ";"; next
}
{ print }
