# Open-source and Maven release procedure

## Source publication

The connector source is published at
[aliyun/maxcompute-connector-for-apache-flink](https://github.com/aliyun/maxcompute-connector-for-apache-flink).
The initial public commit contains a reviewed source snapshot without predecessor
repository history. Source publication does not publish Maven Central artifacts.
Run `python3 tools/check-source.py` and Gitleaks before pushing, then verify the
three-runtime CI results at the exact public commit. Keep credentials, local export
receipts, private CI configuration and binary build outputs outside the source tree.

## Bundled binary and Maven publication

1. Obtain the organizational open-source approval, confirm copyright/provenance and
   approve the source whitelist and third-party redistribution obligations. Resolve
   every `review-required` entry in `licenses/third-party.json`; JavaCsv 2.0 currently
   blocks release. Retain exact corresponding-source/notice proof in the review.
2. Select the actual public repository and release version/tag. Export the reviewed
   commit with `python3 tools/export-public.py --ref <commit> --output <new-directory>`.
   The exporter omits Git history and legacy fixed-environment examples, records file
   hashes, and never pushes. Rebuild this exported tree with a clean Maven repository,
   run `tools/check-public.py --flink-line <line>` for all three runtime lines,
   inspect source/javadoc jars and review the final diff.
3. Run offline and isolated distributed-jar checks, then the two real-service suites
   from `docs/testing.md` on an approved test project. Record commit, JDK/SDK/Flink
   versions, run URLs, counts, fault injection and cleanup/readback. CI compilation,
   historical tests and skipped service tests cannot replace this evidence.
4. Provision Central namespace/token and the approved PGP identity in the protected
   publishing environment. Verify fingerprint, validity and public-key distribution.
   Keep private key/passphrase in secrets. The Maven GPG plugin reads its passphrase
   from `MAVEN_GPG_PASSPHRASE`; never pass it with `-Dgpg.passphrase`.
5. The signing/publishing profile requires verified approval reference, public SCM URL,
   exact tag, signing fingerprint, JDK 17, release dependencies and all test skip flags
   set to false. It also executes the real-service suites. `release-artifacts` must be
   present to attach sources/javadoc; `public` supplies the distributed-jar gate.

```sh
mvn -Ppublic,release-artifacts,release \
  -Dconnector.release.approval="$APPROVAL_REFERENCE" \
  -Dconnector.scm.url="$PUBLIC_SOURCE_URL" \
  -Dconnector.scm.connection="scm:git:$PUBLIC_SOURCE_URL.git" \
  -Dconnector.scm.tag="$RELEASE_TAG" \
  -Dgpg.keyname="$APPROVED_KEY_FINGERPRINT" \
  -Dmaxcompute.e2e.allow-write=true clean verify
for line in 1.16 1.17 1.20; do
  python3 tools/check-public.py --release --flink-line "$line"
done
```

6. Independently run `gpg --verify` on POM, thin/shaded, sources and javadoc signatures
   with the distributed public key. Record SHA-256 for each artifact. Verify that the
   attached test jar, if distributed, contains only approved test material.
7. With explicit publishing authorization, use the protected manual release workflow
   (or the same verified configuration with Maven `deploy`). The Central plugin has
   `autoPublish=false`: inspect/approve the staged deployment in Central before making
   it public. Read back deployment/tag state and download artifacts into a clean
   consumer repository, verify signatures/hashes and rerun consumer acceptance.

Prepared configuration, successful local tests and an internal MR are candidate
milestones. Organizational approval, actual signing identity, Central rights, public
repository/tag, published artifacts and consumer execution are separate release evidence.

Primary references:
- [Maven Shade service/resource transformation](https://maven.apache.org/plugins/maven-shade-plugin/examples/resource-transformers.html)
- [Central publishing with Maven](https://central.sonatype.org/publish/publish-portal-maven/)
- [Maven GPG signing](https://maven.apache.org/plugins/maven-gpg-plugin/sign-mojo.html)

Shaded namespace migration: dependency classes now use the valid Java package
`flink.connector.odps.*`. The old `flink-connector-odps.*` prefix produced invalid
ServiceLoader provider names even after descriptors were merged. Applications
using old shaded class names or serialized dependency classes need compatibility
review before replacing an existing internal deployment; no old-state migration
acceptance is claimed by the candidate tests. Connector-facing package names are unchanged.
