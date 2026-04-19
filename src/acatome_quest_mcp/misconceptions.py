"""Misconception codes and severities — shared vocabulary for reviewers.

A misconception is a factual problem with a paper request: the DOI doesn't
resolve, the title doesn't match the DOI, the paper was retracted, etc.

These codes are part of the MCP contract.  Adding or renaming a code is a
breaking change for any downstream consumer (reviewer agents, Discord bot,
``grimoire/review/review-citations.md``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class MisconceptionCode(StrEnum):
    DOI_INVALID = "doi_invalid"
    DOI_TRUNCATED = "doi_truncated"
    DOI_TITLE_MISMATCH = "doi_title_mismatch"
    TITLE_NOT_FOUND = "title_not_found"
    DUPLICATE_OF = "duplicate_of"
    RETRACTED = "retracted"
    PREPRINT_OF = "preprint_of"


class Severity(StrEnum):
    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"
    INFO = "info"


# Default severity for each code; callers may override per-instance.
DEFAULT_SEVERITY: dict[MisconceptionCode, Severity] = {
    MisconceptionCode.DOI_INVALID: Severity.MAJOR,
    MisconceptionCode.DOI_TRUNCATED: Severity.MAJOR,
    MisconceptionCode.DOI_TITLE_MISMATCH: Severity.CRITICAL,
    MisconceptionCode.TITLE_NOT_FOUND: Severity.CRITICAL,
    MisconceptionCode.DUPLICATE_OF: Severity.MINOR,
    MisconceptionCode.RETRACTED: Severity.CRITICAL,
    MisconceptionCode.PREPRINT_OF: Severity.INFO,
}


@dataclass
class Misconception:
    """A single finding attached to a paper request."""

    code: MisconceptionCode
    severity: Severity
    evidence: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source: str = "resolver"  # resolver, runner, user, reviewer

    @classmethod
    def of(
        cls,
        code: MisconceptionCode | str,
        evidence: str = "",
        *,
        severity: Severity | str | None = None,
        source: str = "resolver",
    ) -> Misconception:
        c = MisconceptionCode(code)
        s = Severity(severity) if severity else DEFAULT_SEVERITY[c]
        return cls(code=c, severity=s, evidence=evidence, source=source)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "severity": self.severity.value,
            "evidence": self.evidence,
            "created_at": self.created_at.isoformat(),
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Misconception:
        ts = d.get("created_at")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        elif ts is None:
            ts = datetime.now(UTC)
        return cls(
            code=MisconceptionCode(d["code"]),
            severity=Severity(d["severity"]),
            evidence=d.get("evidence", ""),
            created_at=ts,
            source=d.get("source", "resolver"),
        )
