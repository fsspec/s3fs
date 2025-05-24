Changelog
=========

2025.5.1
--------

no changes

2025.5.0
--------

- simpler requirements syntax (#958)
- use head_bucket for info(bucket) (#961)

2025.3.2
--------

no changes

2025.3.1
--------

- get_event_loop -> get_running_loop at shutdown (#954)

2025.3.0
--------

- recreate sessino object on refresh (#939)
- re-enable CI tests (#940)

2025.2.0
--------

- update docstrings to new default values (#934)
- fix CI (#936)

2024.12.0
---------

- CI fixes (#922)
- smaller threshold for copy_managed (#921)
- exclusive write (#917)
- fix bug in _find (#913)
- parse query without upstream infer_storage_options (#912)
- bug in _upload_file_part_concurrent (#910)

2024.10.0
---------

- invalidate cache in one-shot pipe file (#904)
- make pipe() concurrent (#901)
- add py3.13 (#898)
- suppoert R2 multi-part uploads (#888)

2024.9.0
--------

no change

2024.6.1
--------

no changes

2024.6.0
--------

no changes

2024.5.0
--------

- widen fsspec req version (#869)
- _bulk_delete must return list (#866)
- retry on "reduce request rate" (#865)

2024.3.1
--------

- accept kwargs in get_file (#863)

2024.3.0
--------

- don't fail ls is parent is unaccessible (#860)
- allow checksum error to retry (#858)
- don't lsbuckets for isdir(bucket) (#856)
- concurrent uplads of parts in put_file (#848)

2024.2.0
--------

- fix cache lookup in _info (#840)

2023.12.2
---------

no changes

2023.12.1
---------

- revert fallback to anon (#835)

2023.12.0
---------

- fall back to anon if no creds are found or passed at all (#823)
- **relax version bounds for aiobotocore** (#829)
- avoid key error if LastModified missing (#828)
- add make_mucket_versioned method (#825)
- retain TZ on modified time (#818)

2023.10.0
---------

- make protocol attribute a tuple (#812)
- update to aiobotocore 2.7.0 (#809)
- fix in _get_file following failure after connect (#805)
- test for du of nonexistent (#803)

2023.9.2
--------

- allow size= in fs.open() (#797)
- rmdir for non-bucket (#975)
- moto updates (#973)
- fix CI warnings (#792)
- dircache usage with depth (#791)

2023.9.1
--------

- retry ClientPayloadError while reading after initial connection (#787)
- don't pass ACL if not specified (#785)

2023.9.0
--------

- aiobotocore to 2.5.4
- better ** support in bulk ops/glob (#769)
- default ACL to "private" rather than blank (#764)
- invalidate cache in rm_file (#762)
- closing client in running loop (#760)

2023.6.0
--------

- allow versions in info.exists (#746)
- streaming file to update it's size for tell (#745, 741)


2023.5.0
--------

- Fix "_" in xattrs tests (#732)
- Fix file pointer already at end of file when retrying put (#731)
- Fix repeated find corrupting cache (#730)
- Remove duplicate class definition (#727)
- return list of deleted keys in bulk deleted (#726)


2023.4.0
--------

- Add streaming async read file (#722)
- Doc fixes (#721)
- aiobotocore to 2.5.0 (#710)

2023.3.0
--------

- Allow setting endpoint_url as top-level kwarg (#704)
- minimum python version 3.8 (#702)
- Update docs config (#697)
- get/put/cp recursive extra tests (#691)

2023.1.0
--------

- parse lambda ARNs (#686)
- recursive on chmod (#679)
- default cache to be readahead (#678)
- temporary redirects in headBucket (#676)
- async iterator for listings (#670)


2022.11.0
---------

- optionally listing versions with ls (#661)

2022.10.0
---------

- directory cache race condition (#655)
- version aware find (#654)

2022.8.1
--------

(no change)

2022.8.0
--------

- aiobotocore 2.4.0 (#643)
- del/list multipart uploads (#645)
- disallow prerelease aiohttp (#640)
- docs syntax (#634)


2022.7.1
--------

No changes

2022.7.0
--------

- aiobotocore 2.3.4 (#633)


2022.5.0
--------

- aiobotocore 2.3 (#622, fixes #558)
- rate limiting (#619, #620)

2022.3.0
--------

- pre-commit (#612)
- aiobotocore 2.2 (#609)
- empty ETag (#605)
- HTTPClientError retry (#597)
- new callbacks support (#590)

2022.02.0
---------

- callbacks fixes (#594, 590)
- drop py36 (#582)
- metadata fixes (#575, 579)

2022.01.0
---------

- aiobotocore dep to 2.1.0 (#564)
- docs for non-aws (#567)
- ContentType in info (#570)
- small-file ACL (#574)

2021.11.1
---------

- deal with missing ETag (#557)
- ClientPayloadError to retryable (#556)
- pin aiobotocore (#555)

2021.11.0
---------

- move to fsspec org
- doc tweaks (#546, 540)
- redondant argument in _rm_versioned_bucket_contents (#439)
- allow client_method in url/sign (POST, etc) (#536)
- revert list_v2->head for info (#545)

2021.10.1
---------

- allow other methods than GET to url/sign (#536)

2021.10.0
---------

No changes (just released to keep pin with fsspec)

2021.09.0
---------

- check for bucket also with get_bucket_location (#533)
- update versioneer (#531)

2021.08.1
---------

- retry on IncompleteRead (#525)
- fix isdir for missing bucket (#522)
- raise for glob("*") (#5167)

2021.08.0
---------

- fix for aiobotocore update (#510)

2021.07.0
---------

- make bucket in put(recursive) (#496)
- non-truthy prefixes (#497)
- implement rm_file (#499)

2021.06.1
---------

- bucket region caching (#495)

2021.06.0
---------

- support "prefix" in directory listings (#486)
- support negative index in cat_file (#487, 488)
- don't requite ETag in file details (#480)

2021.05.0
---------

- optimize ``info``,``exists`` (and related) calls for non-version aware mode
- copy with entries without ETag (#480)
- find not to corrupts parent listing (#476)
- short listing to determine directory (#472, 471)

Version 2021.04.0
-----------------

- switch to calver and fsspec pin
- py36 (#462)
- async fixes (#456, 452)

Version 0.6.0
-------------

- update for fsspec 0.9.0 (#448)
- better errors (#443)
- cp to preserve ETAG (#441)
- CI (#435, #427, #395)
- 5GB PUT (#425)
- partial cat (#389)
- direct find (#360)


Version 0.5.0
-------------

- Asynchronous filesystem based on ``aiobotocore``


Version 0.4.0
-------------

- New instances no longer need reconnect (:pr:`244`) by `Martin Durant`_
- Always use multipart uploads when not autocommitting (:pr:`243`) by `Marius van Niekerk`_
- Create ``CONTRIBUTING.md`` (:pr:`248`) by `Jacob Tomlinson`_
- Use autofunction for ``S3Map`` sphinx autosummary (:pr:`251`) by `James Bourbeau`_
- Miscellaneous doc updates (:pr:`252`) by `James Bourbeau`_
- Support for Python 3.8 (:pr:`264`) by `Tom Augspurger`_
- Improved performance for ``isdir`` (:pr:`259`) by `Nate Yoder`_
- Increased the minimum required version of fsspec to 0.6.0

.. _`Martin Durant`: https://github.com/martindurant
.. _`Marius van Niekerk`: https://github.com/mariusvniekerk
.. _`Jacob Tomlinson`: https://github.com/jacobtomlinson
.. _`James Bourbeau`: https://github.com/jrbourbeau
.. _`Tom Augspurger`: https://github.com/TomAugspurger
.. _`Nate Yoder`: https://github.com/nateyoder
