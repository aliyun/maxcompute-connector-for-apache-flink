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

The first Maven release uses one source tag, `v1.0.0`, for the parent POM and the
three connector modules. Submit all four coordinates in the same Central deployment.
The shaded classifier is the deployable Flink connector for each runtime.

| Artifact in `com.aliyun.odps` | Version |
| --- | --- |
| `flink-connector-maxcompute-parent` | `1.0.0` |
| `flink-connector-maxcompute` | `1.0.0-1.16` |
| `flink-connector-maxcompute` | `1.0.0-1.17` |
| `flink-connector-maxcompute` | `1.0.0-1.20` |

1. Review copyright, source provenance, the source whitelist and third-party
   redistribution obligations. Resolve every `review-required` entry in
   `licenses/third-party.json`. Retain exact corresponding-source and notice
   evidence in the review, including the
   [archived JavaCsv 2.0 source](../licenses/net.sourceforge.javacsv_javacsv_2.0/source-provenance.txt).
2. Select the actual public repository and release version/tag. Export the reviewed
   commit with `python3 tools/export-public.py --ref <commit> --output <new-directory>`.
   The exporter omits Git history and legacy fixed-environment examples, records file
   hashes, and never pushes. Rebuild this exported tree with a clean Maven repository,
   run `tools/check-public.py --flink-line <line>` for all three runtime lines,
   inspect source/javadoc jars and review the final diff.
3. Verify the exact release tag with the [connector CI workflow](../.github/workflows/ci.yml).
   The release workflow runs its source, distributed-jar and Docker Emulator checks
   on the tagged commit before requesting access to the publishing environment.
   All three Flink lines must pass. See [testing](testing.md) for the separate
   real-service suites.
4. Configure required reviewers for the `maven-central` environment. Reviewers
   check the tagged commit and the redistribution evidence before approving staging.
   Configure the Central token as `CENTRAL_USERNAME` and `CENTRAL_PASSWORD` secrets.
   Store one signing key in `GPG_PRIVATE_KEY` and its passphrase in `GPG_PASSPHRASE`.
   Verify key validity and public-key distribution. CI reads the primary fingerprint
   from the imported key. The Maven GPG plugin reads its passphrase from
   `MAVEN_GPG_PASSPHRASE`.
5. The signing/publishing profile requires the public SCM URL, exact tag, signing
   fingerprint, JDK 17, release dependencies and all test skip flags
   set to false. `release-artifacts` attaches sources and Javadoc jars.
   `public` supplies the distributed-jar gate.

```sh
mvn -Ppublic,release-artifacts,release \
  -Dconnector.scm.url="$PUBLIC_SOURCE_URL" \
  -Dconnector.scm.connection="scm:git:$PUBLIC_SOURCE_URL.git" \
  -Dconnector.scm.tag="$RELEASE_TAG" \
  -Dgpg.keyname="$SIGNING_KEY_FINGERPRINT" clean verify
for line in 1.16 1.17 1.20; do
  python3 tools/check-public.py --release --flink-line "$line"
done
```

6. Independently run `gpg --verify` on POM, thin/shaded, sources and javadoc signatures
   with the distributed public key. Record SHA-256 for each artifact. Verify that the
   attached test jar, if distributed, contains only approved test material.
7. With explicit publishing authorization, use the protected manual release workflow.
   With `autoPublish=false`, inspect and approve the staged deployment in Central
   before making it public. Read back deployment/tag state and download artifacts into a clean
   consumer repository, verify signatures/hashes and rerun consumer acceptance.

Prepared configuration, successful local tests and an internal MR are candidate
milestones. Reviewer approval, signing identity, Central rights, public repository/tag,
published artifacts and consumer execution are separate release evidence.

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
