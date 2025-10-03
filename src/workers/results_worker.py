#!/usr/bin/env python3

import os
import logging
from typing import Any, Dict, List, Optional
from base_worker import BaseWorker
from worker_utils import run_main

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_eof_message(message: Any) -> bool:
    """Return True when message carries an EOF control marker."""
    return isinstance(message, dict) and str(message.get("type", "")).upper() == "EOF"


class ResultsWorker(BaseWorker):
    """Worker final que reenvía los resultados procesados hacia el gateway."""

    def _initialize_worker(self):
        self.expected_eof_count = int(os.getenv("EXPECTED_EOF_COUNT", "1"))
        self._eof_seen = 0
        self.amount_results = 0
        self.result_count = 0

    def _tag_transactions_payload(self, payload: Dict[str, Any]) -> bool:
        """Detecta listas de transacciones y ajusta metadatos."""
        data_section = payload.get("data")
        if not isinstance(data_section, list) or not data_section:
            return False

        transactions = [item for item in data_section if isinstance(item, dict) and "transaction_id" in item]
        if not transactions:
            return False

        self.amount_results += len(transactions)
        payload.setdefault("type", "amount_filter_transactions")
        return True

    def _prepare_payload(self, result: Any) -> Optional[Dict[str, Any]]:
        """Normaliza el payload asegurando que tenga metadatos correctos."""
        if isinstance(result, dict):
            if "transaction_id" in result:
                self.amount_results += 1
                logger.debug(
                    "Omitiendo resultado de transaccion individual: %s",
                    result.get("transaction_id"),
                )
                return None

            self._tag_transactions_payload(result)

            if "type" not in result:
                data_section = result.get("data")
                if isinstance(data_section, dict) and "type" in data_section:
                    result["type"] = data_section["type"]

            if "type" not in result:
                result["type"] = "generic_result"

            return result

        if isinstance(result, list):
            if not result:
                return None

            transactions = [item for item in result if isinstance(item, dict) and "transaction_id" in item]
            if transactions:
                self.amount_results += len(transactions)
                return {
                    "type": "amount_filter_transactions",
                    "data": transactions,
                }

            return {
                "type": "batched_results",
                "data": result,
            }

        return {
            "type": "generic_result",
            "data": result,
        }

    def process_message(self, message: Any) -> None:
        """Reenvía un resultado individual al gateway."""
        payload = self._prepare_payload(message)
        if payload is None:
            return

        try:
            self.send_message(payload, client_id=self.current_client_id)
            self.result_count += 1
            logger.info(
                "Resultado #%s reenviado: %s",
                self.result_count,
                getattr(payload, "get", lambda *_: "payload")("type", "payload"),
            )
        except Exception as exc:
            logger.error("Error procesando resultado: %s", exc)

    def process_batch(self, batch: List[Any]) -> None:
        """Procesa un lote de resultados (chunk) como un único payload."""
        self.process_message(batch)

    def _emit_amount_summary(self, client_id: str) -> None:
        try:
            summary = {
                "type": "amount_filter_summary",
                "results_count": self.amount_results,
            }
            self.send_message(summary, client_id=client_id)
            logger.info(
                "Resumen de transacciones filtradas enviado (%s registros)",
                self.amount_results,
            )
        except Exception as exc:
            logger.error("Error enviando resumen de transacciones filtradas: %s", exc)

    def handle_eof(self, message: Dict[str, Any]):
        client_id = message.get("client_id", "") or self.current_client_id
        if client_id:
            self.current_client_id = client_id
        else:
            logger.warning("EOF recibido sin client_id; reenviando sin metadata de cliente")

        self._eof_seen += 1
        logger.info(
                "Recibido EOF en ResultsWorker (%s/%s)",
                self._eof_seen,
                self.expected_eof_count,
            )

        if self._eof_seen >= self.expected_eof_count:
            logger.info("Todos los EOF recibidos; reenviando EOF al gateway")
            try:
                self._emit_amount_summary(client_id)
                self.send_eof(client_id=client_id)
            finally:
                self.input_middleware.stop_consuming()

if __name__ == "__main__":
    run_main(ResultsWorker)
