# Copyright 2016-2026 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
NATS message headers.

NATS headers carry an HTTP/1.1 style status line (``NATS/1.0 <status>
<description>``) followed by an ordered list of header fields. Field
names are case-insensitive and the same field name can appear more than
once on the wire, each occurrence carrying a separate value.

This module provides :class:`Headers`, which preserves every field in
the order received while still behaving like the old ``Dict[str, str]``
mapping (last value wins) used by previous releases.
"""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Dict, Iterable, List, Optional, Tuple, Union

NATS_HDR_LINE = b"NATS/1.0"
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
CRLF = b"\r\n"
CRLF_SIZE = len(CRLF)
SPC = ord(" ")

STATUS_FIELD = "Status"
DESCRIPTION_FIELD = "Description"

HeaderInit = Union[
    "Headers",
    Dict[str, Union[str, List[str]]],
    Iterable[Tuple[str, str]],
]


def _validated(value: str, what: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{what} must be a string")
    if "\r" in value or "\n" in value:
        raise ValueError(f"{what} must not contain CR or LF characters")
    return value


def _status_from(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    if not text.isdigit() or len(text) != 3:
        raise ValueError("status must be a 3 digit code")
    return text


class Headers(MutableMapping):
    """
    Ordered, case-insensitive, multi-valued NATS message headers.

    Mapping access (``headers["Foo"]``) is backwards compatible with the
    previous ``Dict[str, str]`` representation and returns the last value
    stored for a field name. Use :meth:`add` and :meth:`get_all` to work
    with every value of a repeated field, and :meth:`pairs` for all fields
    in their original wire order.

    The inline status line is never mixed with regular fields: it is
    exposed through :attr:`status` and :attr:`description` (also reachable
    as the case-insensitive ``Status``/``Description`` mapping keys) and
    cannot be overwritten by ordinary header fields.
    """

    def __init__(
        self,
        headers: Optional[HeaderInit] = None,
        status: Optional[Union[str, int]] = None,
        description: Optional[str] = None,
    ) -> None:
        self._pairs: List[Tuple[str, str]] = []
        self._status: Optional[str] = _status_from(
            None if status is None else str(status)
        )
        self._description: Optional[str] = (
            None if description in (None, "")
            else _validated(description, "description")
        )

        if headers is None:
            return
        if isinstance(headers, Headers):
            self._pairs = list(headers._pairs)
            if self._status is None:
                self._status = headers._status
            if self._description is None:
                self._description = headers._description
        elif isinstance(headers, dict):
            for key, value in headers.items():
                if isinstance(value, (list, tuple)):
                    for item in value:
                        self.add(key, item)
                else:
                    self.add(key, value)
        else:
            for key, value in headers:
                self.add(key, value)

    @property
    def status(self) -> Optional[str]:
        """Inline status code (e.g. ``"200"``, ``"503"``), if any."""
        return self._status

    @status.setter
    def status(self, value: Optional[Union[str, int]]) -> None:
        self._status = _status_from(None if value is None else str(value))
        if self._status is None:
            self._description = None

    @property
    def description(self) -> Optional[str]:
        """Inline status description, if any."""
        return self._description

    @description.setter
    def description(self, value: Optional[str]) -> None:
        self._description = (
            None if value in (None, "")
            else _validated(value, "description")
        )

    def add(self, key: str, value: str) -> None:
        """
        Appends another value for ``key``. Repeated calls preserve every
        value and the insertion order on the wire.
        """
        if not isinstance(key, str):
            raise TypeError("header name must be a string")
        if not isinstance(value, str):
            raise TypeError("header value must be a string")
        _validated(key, "header name")
        _validated(value, "header value")
        name = key.strip()
        if not name:
            # Skip empty header names for backwards compatibility.
            return
        self._pairs.append((name, value))

    def get_all(self, key: str, default: Optional[List[str]] = None
                ) -> List[str]:
        """
        Returns every value stored for ``key`` (case-insensitive) in the
        original order. ``default`` (or an empty list) is returned when the
        field is absent.
        """
        folded = key.strip().casefold()
        if folded == STATUS_FIELD.casefold():
            return [self._status] if self._status is not None else (
                list(default) if default is not None else []
            )
        if folded == DESCRIPTION_FIELD.casefold():
            return [self._description] if self._description is not None else (
                list(default) if default is not None else []
            )
        values = [v for k, v in self._pairs if k.casefold() == folded]
        return values if values else (
            list(default) if default is not None else []
        )

    def pairs(self) -> List[Tuple[str, str]]:
        """
        Returns all ordinary header fields as ``(name, value)`` tuples in
        the exact order they appeared on the wire, including duplicates.
        """
        return list(self._pairs)

    def copy(self) -> "Headers":
        return Headers(self)

    # MutableMapping interface ------------------------------------------------

    def _field_index(self, folded: str) -> int:
        for index in range(len(self._pairs) - 1, -1, -1):
            if self._pairs[index][0].casefold() == folded:
                return index
        raise KeyError(folded)

    def __getitem__(self, key: str) -> str:
        folded = key.strip().casefold()
        if folded == STATUS_FIELD.casefold():
            if self._status is not None:
                return self._status
            raise KeyError(STATUS_FIELD)
        if folded == DESCRIPTION_FIELD.casefold():
            if self._description is not None:
                return self._description
            raise KeyError(DESCRIPTION_FIELD)
        for name, value in reversed(self._pairs):
            if name.casefold() == folded:
                return value
        raise KeyError(key)

    def __setitem__(self, key: str, value: str) -> None:
        folded = key.strip().casefold()
        if folded == STATUS_FIELD.casefold():
            self.status = value
            return
        if folded == DESCRIPTION_FIELD.casefold():
            self.description = value
            return
        try:
            index = self._field_index(folded)
        except KeyError:
            self.add(key, value)
        else:
            name = key.strip()
            _validated(name, "header name")
            _validated(value, "header value")
            self._pairs[index] = (name, value)

    def __delitem__(self, key: str) -> None:
        folded = key.strip().casefold()
        if folded == STATUS_FIELD.casefold():
            if self._status is None:
                raise KeyError(STATUS_FIELD)
            self._status = None
            self._description = None
            return
        if folded == DESCRIPTION_FIELD.casefold():
            if self._description is None:
                raise KeyError(DESCRIPTION_FIELD)
            self._description = None
            return
        before = len(self._pairs)
        self._pairs = [
            pair for pair in self._pairs
            if pair[0].casefold() != folded
        ]
        if len(self._pairs) == before:
            raise KeyError(key)

    def __iter__(self) -> Iterable[str]:
        seen = set()
        if self._status is not None:
            yield STATUS_FIELD
        if self._description is not None:
            yield DESCRIPTION_FIELD
        for key, _ in self._pairs:
            folded = key.casefold()
            if folded not in seen:
                seen.add(folded)
                yield key

    def __len__(self) -> int:
        count = len({key.casefold() for key, _ in self._pairs})
        if self._status is not None:
            count += 1
        if self._description is not None:
            count += 1
        return count

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        folded = key.strip().casefold()
        if folded == STATUS_FIELD.casefold():
            return self._status is not None
        if folded == DESCRIPTION_FIELD.casefold():
            return self._description is not None
        return any(k.casefold() == folded for k, _ in self._pairs)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Headers):
            return (
                self._status == other._status
                and self._description == other._description
                and [(k.casefold(), v) for k, v in self._pairs]
                == [(k.casefold(), v) for k, v in other._pairs]
            )
        if isinstance(other, dict):
            flat = {key.casefold(): value for key, value in self.items()}
            return flat == {
                str(k).strip().casefold(): v for k, v in other.items()
            }
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    __hash__ = None

    def __repr__(self) -> str:
        return f"<Headers: {dict(self.items())}>"

    # Wire handling -----------------------------------------------------------

    def encode(self) -> bytes:
        """
        Serializes the headers into the NATS header block (without the
        HPUB/HMSG length framing). Byte offsets and lengths are computed
        from the UTF-8 encoded output.
        """
        buf = bytearray()
        buf.extend(NATS_HDR_LINE)
        if self._status is not None:
            buf.append(SPC)
            buf.extend(self._status.encode("utf-8"))
            if self._description:
                buf.append(SPC)
                buf.extend(self._description.encode("utf-8"))
        buf.extend(CRLF)

        suppressed = set()
        if self._status is not None:
            suppressed.add(STATUS_FIELD.casefold())
            suppressed.add(DESCRIPTION_FIELD.casefold())
        for key, value in self._pairs:
            if key.casefold() in suppressed:
                # The real control status owns these names on the wire.
                continue
            buf.extend(key.encode("utf-8"))
            buf.extend(b": ")
            buf.extend(value.encode("utf-8"))
            buf.extend(CRLF)
        buf.extend(CRLF)
        return bytes(buf)

    def __bytes__(self) -> bytes:
        return self.encode()

    @classmethod
    def parse(
        cls,
        raw: bytes,
        on_error=None,
    ) -> Optional["Headers"]:
        """
        Parses a NATS header block as delivered inside an HMSG payload.

        Malformed individual lines are reported through ``on_error`` and
        skipped so that a single bad field cannot discard the remaining
        valid fields (or the messages following it in the read buffer).
        Returns ``None`` for header blocks carrying no usable content.
        """
        try:
            data = bytes(raw)
        except Exception as err:
            if on_error is not None:
                on_error(err)
            return None

        if not data.startswith(NATS_HDR_LINE):
            if on_error is not None:
                on_error(ValueError("nats: malformed headers"))
            return None

        rest = data[NATS_HDR_LINE_SIZE:]
        headers = cls()

        status: Optional[str] = None
        description: Optional[str] = None
        if rest[:1] == b" ":
            # Inline status line: "NATS/1.0 404 No Messages\r\n".
            end = rest.find(CRLF)
            line = rest[1:end if end >= 0 else len(rest)].decode(
                "utf-8", "replace"
            )
            code = line[:3].strip()
            if code.isdigit() and len(code) == 3:
                status = code
                tail = line[3:].strip()
                if tail:
                    description = tail
            rest = rest[end + CRLF_SIZE:] if end >= 0 else b""
        elif rest[:CRLF_SIZE] == CRLF:
            # Plain header block: "NATS/1.0\r\nFoo: bar\r\n...".
            rest = rest[CRLF_SIZE:]
        else:
            # Malformed prefix right after the version.
            if on_error is not None:
                on_error(ValueError("nats: malformed headers"))
            return None

        for line in rest.split(CRLF):
            if not line:
                # Empty line terminates the header block.
                break
            if line.lstrip(b" \t") == b"":
                continue
            colon = line.find(b":")
            if colon < 0:
                # Legacy JetStream heartbeat variant:
                #   NATS/1.0 100\r\nIdle Heartbeat\r\nNats-...: x\r\n
                if status is not None and description is None:
                    description = line.decode("utf-8", "replace").strip()
                else:
                    if on_error is not None:
                        on_error(
                            ValueError(
                                "nats: malformed header line: %r" % line
                            )
                        )
                continue
            try:
                name = line[:colon].decode("utf-8").strip()
                value = line[colon + 1:].decode("utf-8").strip()
            except UnicodeError as err:
                if on_error is not None:
                    on_error(err)
                continue
            if not name:
                if on_error is not None:
                    on_error(ValueError("nats: empty header name"))
                continue
            headers.add(name, value)

        headers._status = status
        headers._description = description
        if not headers._pairs and status is None:
            return None
        return headers
