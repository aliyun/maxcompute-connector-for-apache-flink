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

## Release blocker: JavaCsv 2.0

`net.sourceforge.javacsv:javacsv:2.0` is used by the SDK's SQL CSV readback and cannot
be discarded without breaking it. Central serves the binary but no corresponding
sources/license artifact. The official SourceForge project currently supplies 2.1,
whose source declares LGPL-2.1-or-later; the 2.0 download returns 404 and its binary
hash differs from 2.1. The 2.1 header is retained only as an explicitly labeled
reference. Confirm the exact 2.0 license/corresponding source, or deliver a reviewed
SDK/dependency replacement, before redistributing this jar. `review-required` is a
release failure; a candidate build passing tests does not clear it.

Official project: https://sourceforge.net/projects/javacsv/files/JavaCsv/
Official current source: https://sourceforge.net/projects/javacsv/files/JavaCsv/JavaCsv%202.1/javacsv2.1.zip/download
