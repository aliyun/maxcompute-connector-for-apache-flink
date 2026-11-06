# Third-party redistribution material

`third-party.json` records the exact compile/runtime dependency closure of the public
profile, the binary/source SHA-256 values, source availability, licenses and the
retained license/notice/source-attribution files. Provided Flink dependencies and
test dependencies are not bundled. Run `python3 tools/check-public.py` after
`mvn -Ppublic,release-artifacts clean verify` to detect dependency drift.

Legal material is collected from the exact Central binary/source artifacts; the
additional upstream ANTLR, Protobuf, zstd and JTS texts use the matching release tags.
Original notices/copyright statements remain intact. The project Apache license
applies to connector code; it does not relicense bundled dependencies.

CDDL/GPL alternatives: JavaMail and javax.json include their upstream dual-license
texts; Activation 1.1 retains its CDDL-1.0 text and source reference. AspectJ retains
EPL-2.0, JTS its dual-license attribution and EDL-1.0, ICU its exact Unicode license.
These obligations require the organization's redistribution review before release.

## JavaCsv 2.0

The SDK uses `net.sourceforge.javacsv:javacsv:2.0` for SQL CSV readback. The Fedora
source RPM preserves the original 2.0 archive. Its embedded JAR is byte-identical
to the Central artifact and both library source files declare LGPL-2.1-or-later.
The [provenance record](net.sourceforge.javacsv_javacsv_2.0/source-provenance.txt)
contains the pinned checksums and source extraction command. Include this material
in the organizational redistribution review.
