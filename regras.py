"""
regras.py — Motor de regras para detecção de anomalias agrícolas.

Cada método avalia um DataFrame pandas com leituras recentes e retorna
uma lista de dicts de alerta. O método `avaliar` consolida todos os resultados.

Cada alerta contém além dos campos básicos:
  recomendacoes    — ações que o operador humano deve tomar
  acoes_automaticas — ações já realizadas automaticamente pelo sistema (simuladas)
"""

import pandas as pd


class MotorDeRegras:
    """Aplica regras booleanas sobre o DataFrame de leituras e produz alertas."""

    def verificar_infestacao(self, df: pd.DataFrame) -> list[dict]:
        """
        Regra: (perc_folhas_doentes > 30) AND (praga_detectada != "nenhuma")
        Nível: CRÍTICO — ambas as condições precisam ser verdadeiras simultaneamente.
        """
        alertas = []
        # Máscara booleana: doença grave E praga presente
        mascara = (df["perc_folhas_doentes"] > 30) & (df["praga_detectada"] != "nenhuma")

        for _, linha in df[mascara].iterrows():
            praga = linha["praga_detectada"]
            talhao = int(linha["talhao_id"])
            alertas.append(
                {
                    "regra": "INFESTAÇÃO",
                    "nivel": "CRÍTICO",
                    "talhao_id": talhao,
                    "timestamp": str(linha["timestamp"]),
                    "icone": "🚨",
                    "detalhe": (
                        f"{linha['perc_folhas_doentes']:.1f}% de folhas doentes "
                        f"— praga: {praga}"
                    ),
                    "recomendacoes": [
                        f"Aplicar defensivo agrícola específico para {praga} "
                        "conforme receituário agronômico",
                        f"Isolar o talhão {talhao} e suspender trânsito de máquinas "
                        "até controle confirmado",
                        f"Acionar equipe de campo para inspeção visual imediata "
                        f"no talhão {talhao}",
                        "Documentar ocorrência com fotos e registrar no caderno de campo",
                    ],
                    "acoes_automaticas": [
                        "Notificação de nível CRÍTICO enviada ao gestor da fazenda via sistema",
                        f"Talhão {talhao} marcado como 'Monitoramento Intensivo' "
                        "no mapa de talhões",
                        f"Frequência de leitura de sensores elevada para 60 s no talhão {talhao}",
                        "Registro de ocorrência de praga gerado no histórico do sistema",
                    ],
                }
            )
        return alertas

    def verificar_irrigacao(self, df: pd.DataFrame) -> list[dict]:
        """
        Regra: (umidade_solo_pct < 30) AND (nivel_irrigacao == "baixo")
        Nível: ATENÇÃO — solo seco E irrigação insuficiente ao mesmo tempo.
        """
        alertas = []
        # Máscara booleana: solo seco E irrigação já na posição baixa
        mascara = (df["umidade_solo_pct"] < 30) & (df["nivel_irrigacao"] == "baixo")

        for _, linha in df[mascara].iterrows():
            talhao = int(linha["talhao_id"])
            umidade = linha["umidade_solo_pct"]
            alertas.append(
                {
                    "regra": "IRRIGAÇÃO",
                    "nivel": "ATENÇÃO",
                    "talhao_id": talhao,
                    "timestamp": str(linha["timestamp"]),
                    "icone": "💧",
                    "detalhe": (
                        f"Umidade em {umidade:.1f}% com irrigação baixa"
                    ),
                    "recomendacoes": [
                        f"Acionar irrigação suplementar no talhão {talhao} imediatamente",
                        "Verificar integridade das linhas de gotejamento e aspersores do setor",
                        "Inspecionar o sensor de umidade — possível falha de calibração",
                        "Consultar previsão do tempo: se chuva prevista em < 24 h, "
                        "aguardar antes de irrigar",
                    ],
                    "acoes_automaticas": [
                        "Solicitação de irrigação emergencial enviada ao controlador de campo",
                        f"Alerta de solo seco registrado no histórico do talhão {talhao}",
                        "Equipe de manutenção notificada sobre possível falha no sistema "
                        "de irrigação",
                        "Prioridade do talhão elevada no índice de monitoramento",
                    ],
                }
            )
        return alertas

    def verificar_temperatura(self, df: pd.DataFrame) -> list[dict]:
        """
        Regra: (temperatura_c > 35) OR (temperatura_c < 12)
        Nível: AVISO — basta UMA das condições para considerar fora da faixa segura.
        Recomendações variam conforme calor extremo ou frio extremo.
        """
        alertas = []
        # Máscara booleana: calor extremo OU frio extremo
        mascara = (df["temperatura_c"] > 35) | (df["temperatura_c"] < 12)

        for _, linha in df[mascara].iterrows():
            talhao = int(linha["talhao_id"])
            temp = linha["temperatura_c"]
            calor = temp > 35

            if calor:
                recomendacoes = [
                    "Avaliar irrigação noturna para resfriamento do solo",
                    "Verificar necessidade de sombreamento temporário para culturas sensíveis",
                    "Consultar agrônomo para avaliação de risco de queima foliar",
                    "Monitorar previsão de temperaturas nas próximas 48 horas",
                ]
            else:
                recomendacoes = [
                    "Verificar necessidade de cobertura protetora (mulching / manta agrícola)",
                    "Antecipar colheita de culturas sensíveis ao frio se possível",
                    "Inspecionar sistemas de irrigação por risco de congelamento nas tubulações",
                    "Consultar agrônomo para avaliação de risco de geada",
                ]

            alertas.append(
                {
                    "regra": "TEMPERATURA",
                    "nivel": "AVISO",
                    "talhao_id": talhao,
                    "timestamp": str(linha["timestamp"]),
                    "icone": "🌡️",
                    "detalhe": (
                        f"Temperatura de {temp:.1f}°C fora da faixa segura"
                    ),
                    "recomendacoes": recomendacoes,
                    "acoes_automaticas": [
                        "Alerta climático registrado no log de anomalias do sistema",
                        "Relatório de anomalia de temperatura gerado automaticamente",
                        "Notificação enviada ao responsável técnico da área",
                        f"Frequência de monitoramento do talhão {talhao} "
                        "aumentada pelos próximos 30 min",
                    ],
                }
            )
        return alertas

    def avaliar(self, df: pd.DataFrame) -> list[dict]:
        """
        Consolida os resultados de todas as regras, ordena por timestamp
        descendente e retorna lista única de alertas.
        """
        if df.empty:
            return []

        todos = (
            self.verificar_infestacao(df)
            + self.verificar_irrigacao(df)
            + self.verificar_temperatura(df)
        )

        # Ordena do mais recente para o mais antigo
        todos.sort(key=lambda a: a["timestamp"], reverse=True)
        return todos
