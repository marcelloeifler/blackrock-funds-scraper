import hashlib
import logging
import os
import re
from datetime import date, datetime, timedelta
from html import unescape
from typing import Optional, Tuple, Union

import pandas as pd
from dateparser import parse

log = logging.getLogger(__name__)


def gera_pasta_destino_preco_fechamento(pasta_destino: str, tipo: str) -> str:
    return os.path.join(pasta_destino, tipo)


def adiciona_fuso(
    last_trade_mic_time: str, diferenca_horas: int, diferenca_minutos: int
) -> str:
    """
    Ajusta o horário aplicando diferença positiva ou negativa.

    Aceita:
      - last_trade_mic_time: str "YYYY-MM-DD HH:MM:SS", pandas.Timestamp ou datetime.datetime
      - diferenca_horas: int, numpy.int64, etc.
      - diferenca_minutos: int, numpy.int64, etc.
    """

    # --- Normaliza o tipo do horário ---
    if isinstance(last_trade_mic_time, datetime):
        dt = last_trade_mic_time

    elif isinstance(last_trade_mic_time, pd.Timestamp):
        dt = last_trade_mic_time.to_pydatetime()

    elif isinstance(last_trade_mic_time, str):
        dt = datetime.strptime(last_trade_mic_time, "%Y-%m-%d %H:%M:%S")

    else:
        raise TypeError(
            f"Tipo não suportado para last_trade_mic_time: {type(last_trade_mic_time)}"
        )

    # --- Normaliza tipos numéricos (int, numpy.int64, etc.) ---
    try:
        horas = int(diferenca_horas)
        minutos = int(diferenca_minutos)
    except Exception as e:
        raise TypeError(
            f"Tipos inválidos para diferenca_horas/minutos: "
            f"{type(diferenca_horas)}, {type(diferenca_minutos)}"
        ) from e

    # --- Aplica o delta ---
    delta = timedelta(hours=horas, minutes=minutos)
    dt_corrigido = dt + delta

    return dt_corrigido.strftime("%Y-%m-%d %H:%M:%S")


def limpa_texto_resposta(texto_raw: str) -> str:
    # Converte entidades HTML (ex: &#160;)
    texto = unescape(texto_raw)

    # Remove soft hyphen (SHY)
    texto = texto.replace("\u00ad", "")

    # Converte NBSP (não-quebra-de-linha) para espaço normal
    texto = texto.replace("\xa0", " ")

    # Remove outros caracteres invisíveis problemáticos
    texto = texto.replace("\u200b", "")  # zero width space
    texto = texto.replace("\uFEFF", "")  # BOM invisível

    return texto


def filtrar_numeros_e_caracteres(texto: str, caracteres: list):
    if pd.isna(texto):
        return texto

    return "".join(ch for ch in texto if ch.isdigit() or ch in caracteres)


def converter_data_generica(data_str):
    if pd.isna(data_str):
        return None

    dt = parse(data_str, languages=["pt"])

    return dt.strftime("%Y-%m-%d")


def convert_values(
    value: Optional[str], aceitar_negativo: bool = False
) -> Optional[float]:
    """
    Conversor de valores

    Parameters
    ----------
    value: string ou None
    aceitar_negativo: se True, permite valores negativos

    Returns
    -------
    float se possível, senão None
    """
    try:
        if value is None:
            return None

        value = value.strip()

        # remove espaços, vírgulas e pontos apenas para validar se é numérico
        pattern_numeric = r"[ ,.]"
        cleaned = re.sub(pattern_numeric, "", value)

        if aceitar_negativo:
            # para validação, aceita um '-' apenas no início
            if cleaned.startswith("-"):
                cleaned_to_check = cleaned[1:]
            else:
                cleaned_to_check = cleaned
        else:
            # se não aceita negativo, qualquer '-' torna inválido
            if "-" in cleaned:
                return None
            cleaned_to_check = cleaned

        if not cleaned_to_check.isnumeric():
            return None

        # Padrões para identificar casas decimais
        pattern_dot = r"\.\d{2,}$"
        pattern_comma = r",\d{2,}$"

        if re.search(pattern_dot, value):
            # Ex: "1.234,56" -> "1234.56"
            return float(value.replace(",", "").replace(" ", ""))

        if re.search(pattern_comma, value):
            # Ex: "1.234,56" -> "1234.56"
            return float(value.replace(".", "").replace(" ", "").replace(",", "."))

        # Sem vírgula como decimal, tenta converter direto (removendo só espaços)
        return float(value.replace(" ", ""))

    except Exception:
        log.exception(f"'{value}' não é um número válido.")
        raise


def trata_coluna_numerica(df: pd.DataFrame, lista_colunas: list) -> pd.DataFrame:
    for coluna in lista_colunas:
        df[coluna] = df[coluna].astype(str)
        df[coluna] = df[coluna].apply(
            lambda x: filtrar_numeros_e_caracteres(x, [",", "."])
        )
        df[coluna] = df[coluna].apply(convert_values)

    return df


def substituir_vazios_por_none(df):
    lista_nulos = ["", "-", "n/a"]
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: (
                None
                if isinstance(x, str) and x.strip().lower() in lista_nulos
                else x.strip() if isinstance(x, str) else x
            )
        )
    return df


def gera_data_atualizacao(padrao: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now().strftime(padrao)
