from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

NATS_HDR_LINE = b"NATS/1.0"
_CRLF_ = b"\r\n"
_SPC_ = b" "

STATUS = "Status"
DESCRIPTION = "Description"

HeaderValue = Union[str, List[str], Tuple[str, ...]]
HeaderInput = Union["Headers", Dict[str, HeaderValue], Iterable[Tuple[str, HeaderValue]]]


class Headers(MutableMapping):
    """
    Case-insensitive NATS header mapping which preserves repeated values.

    Dictionary-style reads retain the historical behavior of returning the
    last value. The status line is represented separately internally while
    remaining visible through the legacy Status/Description keys.
    """

    @classmethod
    def from_wire(cls, data: bytes) -> "Headers":
        return headers_from_wire(data)

    def __init__(
        self,
        headers: Optional[HeaderInput] = None,
        *,
        status: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        self._items: List[Tuple[str, str]] = []
        self._wire: Optional[bytes] = None
        self.status = status
        self.description = description
        if headers is not None:
            self.update(headers)

    @staticmethod
    def _normalized(name: str) -> str:
        return name.lower()

    def _find(self, name: str) -> int:
        wanted = self._normalized(name)
        for index, (key, _) in enumerate(self._items):
            if self._normalized(key) == wanted:
                return index
        raise KeyError(name)

    def __getitem__(self, name: str) -> str:
        normalized = self._normalized(name)
        if normalized == self._normalized(STATUS) and self.status is not None:
            return self.status
        if (
            normalized == self._normalized(DESCRIPTION)
            and self.description is not None
        ):
            return self.description
        for index in range(len(self._items) - 1, -1, -1):
            key, value = self._items[index]
            if self._normalized(key) == self._normalized(name):
                return value
        raise KeyError(name)

    def __setitem__(self, name: str, value: HeaderValue) -> None:
        if isinstance(value, (list, tuple)):
            values = list(value)
        else:
            values = [value]
        if not values:
            self.__delitem__(name)
            return
        self._assign(name, [str(item) for item in values])

    def _assign(self, name: str, values: List[str]) -> None:
        self._wire = None
        normalized = self._normalized(name)
        if normalized == self._normalized(STATUS):
            self.status = values[-1]
            return
        if normalized == self._normalized(DESCRIPTION) and self.status is not None:
            self.description = values[-1]
            return
        remaining = list(values[1:])
        replaced = False
        new_items: List[Tuple[str, str]] = []
        for key, old_value in self._items:
            if self._normalized(key) == normalized:
                if not replaced:
                    new_items.append((name, values[0]))
                    replaced = True
                elif remaining:
                    new_items.append((name, remaining.pop(0)))
                else:
                    continue
            else:
                new_items.append((key, old_value))
        if not replaced:
            new_items.append((name, values[0]))
        for value in remaining:
            new_items.append((name, value))
        self._items = new_items

    def __delitem__(self, name: str) -> None:
        self._wire = None
        normalized = self._normalized(name)
        if normalized == self._normalized(STATUS):
            self.status = None
            return
        if normalized == self._normalized(DESCRIPTION):
            self.description = None
            if not any(
                self._normalized(key) == normalized for key, _ in self._items
            ):
                return
        index = self._find(name)
        del self._items[index]

    def __iter__(self) -> Iterator[str]:
        seen = set()
        if self.status is not None:
            yield STATUS
            seen.add(self._normalized(STATUS))
        if self.description is not None:
            yield DESCRIPTION
            seen.add(self._normalized(DESCRIPTION))
        for key, _ in self._items:
            normalized = self._normalized(key)
            if normalized not in seen:
                seen.add(normalized)
                yield key

    def __len__(self) -> int:
        length = len(set(self._normalized(key) for key, _ in self._items))
        if self.status is not None and STATUS.lower() not in {
            self._normalized(key) for key, _ in self._items
        }:
            length += 1
        if self.description is not None and DESCRIPTION.lower() not in {
            self._normalized(key) for key, _ in self._items
        }:
            length += 1
        return length

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        normalized = self._normalized(name)
        has_field = any(
            self._normalized(key) == normalized for key, _ in self._items
        )
        if has_field:
            return True
        if normalized == self._normalized(STATUS):
            return self.status is not None
        if normalized == self._normalized(DESCRIPTION):
            return self.description is not None
        return False

    def __eq__(self, other: object) -> bool:
        try:
            if isinstance(other, Headers):
                other_items = [(key, other[key]) for key in other]
            elif hasattr(other, "items"):
                other_items = list(other.items())
            else:
                other_items = list(other)  # type: ignore[call-overload]
        except TypeError:
            return NotImplemented
        normalized = {
            self._normalized(key): value for key, value in self.items()
        }
        other_normalized = {
            self._normalized(key): value for key, value in other_items
        }
        return normalized == other_normalized

    __hash__ = None  # type: ignore[assignment]

    def add(self, name: str, value: str) -> None:
        self._wire = None
        normalized = self._normalized(name)
        if normalized == self._normalized(STATUS):
            self.status = str(value)
        elif (
            normalized == self._normalized(DESCRIPTION)
            and self.status is not None
        ):
            self.description = str(value)
        else:
            self._items.append((name, str(value)))

    def _add_parsed_field(self, name: str, value: str) -> None:
        self._items.append((name, str(value)))

    def get_all(self, name: str) -> List[str]:
        normalized = self._normalized(name)
        return [
            value
            for key, value in self._items
            if self._normalized(key) == normalized
        ]

    def getlist(self, name: str) -> List[str]:
        return self.get_all(name)

    def multi_items(self) -> List[Tuple[str, str]]:
        return list(self._items)

    def raw_items(self) -> List[Tuple[str, str]]:
        return self.multi_items()

    def all_items(self) -> List[Tuple[str, str]]:
        return self.multi_items()

    def items_multi(self) -> List[Tuple[str, str]]:
        return self.multi_items()

    def wire_items(self) -> List[Tuple[str, str]]:
        items: List[Tuple[str, str]] = []
        if self.status is not None:
            items.append((STATUS, self.status))
        if self.description is not None:
            items.append((DESCRIPTION, self.description))
        items.extend(self._items)
        return items

    def items(self):  # type: ignore[override]
        return super().items()

    def copy(self) -> "Headers":
        headers = Headers()
        headers._items = list(self._items)
        headers._wire = self._wire
        headers.status = self.status
        headers.description = self.description
        return headers

    def to_wire(self) -> bytes:
        if self._wire is not None:
            return self._wire

        status_line = NATS_HDR_LINE
        if self.status is not None:
            status_line += _SPC_ + _validate_field(self.status).encode()
            if self.description is not None:
                status_line += _SPC_ + _validate_field(self.description).encode()

        lines = [status_line + _CRLF_]
        for key, value in self._items:
            lines.append(
                _validate_field(key).encode()
                + b": "
                + _validate_field(value).encode()
                + _CRLF_
            )
        lines.append(_CRLF_)
        return b"".join(lines)


def _validate_field(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("header names and values must be strings")
    if "\r" in value or "\n" in value:
        raise ValueError("header names and values must not contain newlines")
    return value


def parse_headers(data: bytes) -> Headers:
    if not data:
        return Headers()
    if not data.startswith(NATS_HDR_LINE):
        raise ValueError("invalid NATS header status line")

    pos = len(NATS_HDR_LINE)
    status: Optional[str] = None
    description: Optional[str] = None

    if pos < len(data) and data[pos:pos + 1] == _SPC_:
        line_end = data.find(_CRLF_, pos)
        if line_end < 0:
            raise ValueError("invalid NATS header status line")
        status_line = data[pos + 1:line_end]
        status_text = status_line[:3].decode("ascii")
        if len(status_text) != 3 or not status_text.isdigit():
            raise ValueError("invalid NATS header status code")
        status = status_text
        if len(status_line) > 3:
            if status_line[3:4] != _SPC_:
                raise ValueError("invalid NATS header status line")
            description = status_line[4:].decode("utf-8")
        pos = line_end + len(_CRLF_)
    else:
        if data[pos:pos + len(_CRLF_)] != _CRLF_:
            raise ValueError("invalid NATS header status line")
        pos += len(_CRLF_)

    headers = Headers(status=status, description=description)
    while pos < len(data):
        line_end = data.find(_CRLF_, pos)
        if line_end < 0:
            raise ValueError("unterminated NATS header")
        line = data[pos:line_end]
        pos = line_end + len(_CRLF_)
        if not line:
            if pos != len(data):
                raise ValueError("unexpected data after NATS headers")
            return headers

        separator = line.find(b":")
        if separator <= 0:
            raise ValueError("invalid NATS header field")
        name = line[:separator].decode("utf-8")
        value = line[separator + 1:]
        if value.startswith(_SPC_):
            value = value[1:]
        headers._add_parsed_field(name, value.decode("utf-8"))

    raise ValueError("unterminated NATS headers")


def headers_from_wire(data: bytes) -> Headers:
    headers = parse_headers(data)
    headers._wire = bytes(data)
    return headers
