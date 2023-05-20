Merging files
-------------

You can't really merge files with only two samples.

Consider this example:

1. File ``a.txt`` is created and pushed to storage.

    .. code-block:: python

        Line 1.
        Line 2.
        Line 3.

2. User x, and user y, both download the file, and make changes to it.

   User x (and changes from their original file):

    .. code-block:: python

        Line 1 (user x).
        Line 3.
        Line 4. (user x)

        - Line 1.
        + Line 1 (user x).
        - Line 2.
          Line 3.
        + Line 4. (user x)

    User y (and changes from their original file):

    .. code-block:: python

        Line 1 (user y).
        Line 2.
        Line 3.
        Line 4. (user y)

        - Line 1.
        + Line 1 (user y).
          Line 2.
          Line 3.
        + Line 4. (user y)

3. User x, pushes his changes to storage.
4. Can user y automatically merge his changes with the version in storage?

The answer is no.

User y's file has a newer timestamp than the version in storage,
but since we don't have access to a "base" version, we cannot determine
which changes are divergent.

E.g. we have no way of knowing if ``Line 2.`` is a new line added by user y,
instead of a line that was removed by user x.

Conclusion
~~~~~~~~~~

We can only rely on the timestamp and hash of the file to determine if they are
the same, and which should be the new version, i.e.:

- If the hash matches, then update the local timestamp to the timestamp in storage.
- If the hash doesn't match,

  * if the timestamp in storage is newer: then download the version in storage, and set the
    local timestamp to the timestamp in storage.
  * if the timestamp in storage is older: then upload the local version to storage, and set the
    timestamp in storage to the local timestamp.

