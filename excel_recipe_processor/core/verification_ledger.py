"""
Run-level ledger of verification outcomes.

excel_recipe_processor/core/verification_ledger.py

verify_data records each rule's outcome here; the pipeline logs one
summary line at run end so the answer to "did anything warn?" never
requires scrolling the log. Class-level state, reset by the pipeline at
run start, mirroring how WorkbookSession scopes its lifecycle.
"""

import logging


logger = logging.getLogger(__name__)


class VerificationLedger:
    """Accumulates rule outcomes across a run for the closing summary."""

    _passed: int = 0
    _warned: int = 0
    _halted: int = 0

    @classmethod
    def reset(cls) -> None:
        """Start a run with a clean ledger."""
        cls._passed = 0
        cls._warned = 0
        cls._halted = 0

    @classmethod
    def record_pass(cls) -> None:
        cls._passed += 1

    @classmethod
    def record_warn(cls) -> None:
        cls._warned += 1

    @classmethod
    def record_halt(cls) -> None:
        cls._halted += 1

    @classmethod
    def has_entries(cls) -> bool:
        return (cls._passed + cls._warned + cls._halted) > 0

    @classmethod
    def log_summary(cls) -> None:
        """One line at run end; silent when no verifications ran."""
        if not cls.has_entries():
            return

        if cls._warned or cls._halted:
            logger.warning(
                f"🔎 Verifications: {cls._passed} passed, {cls._warned} warned"
                + (f", {cls._halted} halted" if cls._halted else "")
                + " - see the ⚠️ lines above for the details"
            )
        else:
            logger.info(f"🔎 Verifications: all {cls._passed} rule(s) passed")

# End of file #
