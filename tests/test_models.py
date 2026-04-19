"""Tests for models, normalization, and misconception round-tripping."""

from __future__ import annotations

from acatome_quest_mcp.misconceptions import (
    DEFAULT_SEVERITY,
    Misconception,
    MisconceptionCode,
    Severity,
)
from acatome_quest_mcp.models import (
    PaperRef,
    normalize_arxiv,
    normalize_doi,
)


class TestDoiNormalization:
    def test_plain_doi(self) -> None:
        assert normalize_doi("10.1021/jacs.2c01234") == "10.1021/jacs.2c01234"

    def test_uppercase_doi_lowercased(self) -> None:
        assert normalize_doi("10.1021/JACS.2C01234") == "10.1021/jacs.2c01234"

    def test_doi_org_url_prefix_stripped(self) -> None:
        assert (
            normalize_doi("https://doi.org/10.1021/jacs.2c01234")
            == "10.1021/jacs.2c01234"
        )

    def test_dx_doi_org_url_stripped(self) -> None:
        assert (
            normalize_doi("http://dx.doi.org/10.1039/C4EE01303D")
            == "10.1039/c4ee01303d"
        )

    def test_doi_colon_prefix_stripped(self) -> None:
        assert normalize_doi("doi:10.1023/A:1020527214182") == "10.1023/a:1020527214182"

    def test_trailing_punctuation_stripped(self) -> None:
        assert normalize_doi("10.1021/jacs.2c01234.") == "10.1021/jacs.2c01234"
        assert normalize_doi("10.1021/jacs.2c01234),") == "10.1021/jacs.2c01234"

    def test_junk_returns_none(self) -> None:
        assert normalize_doi("not a doi") is None
        assert normalize_doi("") is None
        assert normalize_doi(None) is None

    def test_kit_repository_handle_rejected(self) -> None:
        # From giri/corrections.md: "KIT repository handle, not a CrossRef DOI"
        # It matches our regex but Crossref will reject it — that's the
        # resolver's job, not normalization's.
        assert normalize_doi("10.5445/IR/1000175212") == "10.5445/ir/1000175212"


class TestArxivNormalization:
    def test_plain_id(self) -> None:
        assert normalize_arxiv("2508.20254") == "2508.20254"

    def test_id_with_version(self) -> None:
        assert normalize_arxiv("2508.20254v1") == "2508.20254v1"

    def test_arxiv_colon_prefix(self) -> None:
        assert normalize_arxiv("arXiv:2301.12345") == "2301.12345"

    def test_abs_url(self) -> None:
        assert normalize_arxiv("https://arxiv.org/abs/2301.12345v2") == "2301.12345v2"

    def test_pdf_url(self) -> None:
        assert normalize_arxiv("https://arxiv.org/pdf/2301.12345.pdf") == "2301.12345"

    def test_old_style_id(self) -> None:
        assert normalize_arxiv("cond-mat/0102536") == "cond-mat/0102536"

    def test_junk_returns_none(self) -> None:
        assert normalize_arxiv("not an arxiv id") is None
        assert normalize_arxiv("") is None


class TestPaperRefNormalize:
    def test_normalize_cleans_fields(self) -> None:
        ref = PaperRef(
            doi="  https://DOI.ORG/10.1021/JACS.2C01234  ",
            title="  Hello World  ",
            authors=["", "  Smith, J.  ", None],  # type: ignore[list-item]
        )
        out = ref.normalize()
        assert out.doi == "10.1021/jacs.2c01234"
        assert out.title == "Hello World"
        assert out.authors == ["Smith, J."]

    def test_raw_doi_promoted(self) -> None:
        ref = PaperRef(
            raw="Feng et al. 2024, doi:10.1016/j.memsci.2020.118000 something"
        )
        out = ref.normalize()
        assert out.doi == "10.1016/j.memsci.2020.118000"

    def test_raw_arxiv_promoted(self) -> None:
        ref = PaperRef(raw="See arXiv:2508.20254 for details")
        out = ref.normalize()
        assert out.arxiv == "2508.20254"

    def test_is_empty(self) -> None:
        assert PaperRef().is_empty()
        assert not PaperRef(doi="10.1/x").is_empty()
        assert not PaperRef(title="foo").is_empty()

    def test_from_dict_accepts_paper_ref(self) -> None:
        ref = PaperRef(doi="10.1/x")
        assert PaperRef.from_dict(ref) is ref

    def test_from_dict_accepts_str_author(self) -> None:
        out = PaperRef.from_dict({"title": "foo", "authors": "Smith, J."})
        assert out.authors == ["Smith, J."]


class TestMisconceptions:
    def test_default_severity(self) -> None:
        m = Misconception.of(MisconceptionCode.DOI_TITLE_MISMATCH)
        assert m.severity == Severity.CRITICAL
        assert m.severity == DEFAULT_SEVERITY[MisconceptionCode.DOI_TITLE_MISMATCH]

    def test_explicit_severity_wins(self) -> None:
        m = Misconception.of(
            MisconceptionCode.DOI_INVALID,
            severity=Severity.MINOR,
            evidence="only a warning",
        )
        assert m.severity == Severity.MINOR

    def test_accepts_string_codes(self) -> None:
        m = Misconception.of("retracted", evidence="RW 2024")
        assert m.code == MisconceptionCode.RETRACTED
        assert m.severity == Severity.CRITICAL

    def test_round_trip(self) -> None:
        m = Misconception.of(
            MisconceptionCode.DUPLICATE_OF,
            evidence="slug wang2020state",
            source="resolver",
        )
        d = m.to_dict()
        assert d["code"] == "duplicate_of"
        m2 = Misconception.from_dict(d)
        assert m2.code == m.code
        assert m2.severity == m.severity
        assert m2.evidence == m.evidence
        assert m2.source == m.source

    def test_every_code_has_default_severity(self) -> None:
        for code in MisconceptionCode:
            assert code in DEFAULT_SEVERITY, f"{code} missing default severity"
