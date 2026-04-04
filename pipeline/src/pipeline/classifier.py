"""NLP classifier for human waste detection using spaCy."""

from dataclasses import dataclass, field
from typing import Any

import spacy
from spacy.tokens import Doc

from pipeline.config import (
    BPW_REJECTION_PATTERN,
    CONTEXT_BOOSTERS,
    FALSE_POSITIVE_CONTEXTS,
    HIGH_SIGNAL_LEMMAS,
    HIGH_SIGNAL_PHRASES,
    MEDIUM_SIGNAL_LEMMAS,
)


@dataclass
class ClassificationResult:
    """Result of classifying a single record."""

    case_id: str
    score: float  # 0.0 to 1.0
    confidence: str  # "high", "medium", "low", "none"
    matched_terms: list[str] = field(default_factory=list)
    matched_phrases: list[str] = field(default_factory=list)
    context_boosters: list[str] = field(default_factory=list)
    false_positive_flags: list[str] = field(default_factory=list)
    bpw_rejection: bool = False
    source_texts: dict[str, str] = field(default_factory=dict)


class WasteClassifier:
    """Classifies 311 records for human waste content using spaCy NLP."""

    def __init__(self) -> None:
        self.nlp: spacy.language.Language = spacy.load("en_core_web_sm", disable=["ner", "parser"])

    def _get_lemmas(self, text: str) -> list[str]:
        """Tokenize and lemmatize text, returning list of lowercase lemmas."""
        doc: Doc = self.nlp(text.lower())
        return [token.lemma_ for token in doc if not token.is_punct and not token.is_space]

    def _check_phrases(self, text: str) -> list[str]:
        """Check for multi-word phrases in the raw lowercased text."""
        lower_text = text.lower()
        found = []
        for phrase in HIGH_SIGNAL_PHRASES:
            if phrase in lower_text:
                found.append(phrase)
        return found

    def _check_bpw_rejection(self, text: str) -> bool:
        """Check for the standard BPW rejection phrase."""
        return BPW_REJECTION_PATTERN in text.lower()

    def classify_text(self, text: str) -> tuple[list[str], list[str], list[str], list[str], bool]:
        """Classify a single text string.

        Returns: (high_matches, medium_matches, boosters, fp_flags, bpw_rejection)
        """
        lemmas = self._get_lemmas(text)
        lemma_set = set(lemmas)

        high_matches = sorted(lemma_set & HIGH_SIGNAL_LEMMAS)
        medium_matches = sorted(lemma_set & MEDIUM_SIGNAL_LEMMAS)
        boosters = sorted(lemma_set & CONTEXT_BOOSTERS)
        fp_flags = sorted(lemma_set & FALSE_POSITIVE_CONTEXTS)
        bpw = self._check_bpw_rejection(text)

        return high_matches, medium_matches, boosters, fp_flags, bpw

    def classify_record(self, record: dict[str, Any]) -> ClassificationResult:
        """Classify a 311 record for human waste content."""
        case_id = str(record.get("case_enquiry_id", "unknown"))

        # Gather all text fields to analyze
        texts: dict[str, str] = {}
        closure = record.get("closure_reason") or ""
        if closure:
            texts["closure_reason"] = closure

        description = record.get("open311_description") or ""
        if description:
            texts["open311_description"] = description

        # Also check the case_title if available
        title = record.get("case_title") or ""
        if title:
            texts["case_title"] = title

        if not texts:
            return ClassificationResult(case_id=case_id, score=0.0, confidence="none")

        # Combine all text for analysis
        combined = " ".join(texts.values())

        high_matches, medium_matches, boosters, fp_flags, bpw = self.classify_text(combined)
        phrases = self._check_phrases(combined)

        # Scoring logic
        score = 0.0

        # BPW rejection is very strong signal
        if bpw:
            score += 0.8

        # High-signal terms
        score += len(high_matches) * 0.3
        score += len(phrases) * 0.4

        # Medium-signal terms with context
        if medium_matches:
            if boosters and not fp_flags:
                score += len(medium_matches) * 0.2
            elif not fp_flags:
                score += len(medium_matches) * 0.1
            # With false positive flags, medium signals get minimal weight
            else:
                score += len(medium_matches) * 0.02

        # Context boosters only matter if there's already some signal
        if high_matches or medium_matches or phrases or bpw:
            score += len(boosters) * 0.05

        # False positive flags reduce score — but not if we have explicit
        # phrases like "human poop" or BPW rejection, since those already
        # disambiguate from animal waste
        if fp_flags and not phrases and not bpw:
            score *= 0.3

        # Cap at 1.0
        score = min(score, 1.0)

        # Determine confidence level
        if score >= 0.6:
            confidence = "high"
        elif score >= 0.3:
            confidence = "medium"
        elif score > 0.0:
            confidence = "low"
        else:
            confidence = "none"

        return ClassificationResult(
            case_id=case_id,
            score=round(score, 3),
            confidence=confidence,
            matched_terms=high_matches + medium_matches,
            matched_phrases=phrases,
            context_boosters=boosters,
            false_positive_flags=fp_flags,
            bpw_rejection=bpw,
            source_texts=texts,
        )

    def classify_batch(self, records: list[dict[str, Any]]) -> list[ClassificationResult]:
        """Classify a batch of records."""
        return [self.classify_record(r) for r in records]
