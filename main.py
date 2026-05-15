"""
main.py — Ponto de entrada do sistema de monitoramento agrícola.

Inicia uma thread de geração contínua de dados e expõe um servidor Flask
com dashboard HTML embutido e API JSON para consulta em tempo real.

Uso:
    pip install flask pandas
    python main.py
    → http://localhost:5000
"""

import os
import threading
import time

import pandas as pd
from flask import Flask, jsonify, render_template_string, request

from gerador import gerar_leitura_forcada, gerar_leitura_normal, salvar_no_csv, set_lock
from regras import MotorDeRegras

# ---------------------------------------------------------------------------
# Configuração global
# ---------------------------------------------------------------------------

CSV_PATH = "dados/leituras.csv"
os.makedirs("dados", exist_ok=True)

# Lock único compartilhado entre Flask e a thread de geração
csv_lock = threading.Lock()
set_lock(csv_lock)  # injeta o lock no módulo gerador

app = Flask(__name__)
motor = MotorDeRegras()

# Lista em memória com os alertas avaliados mais recentemente (máx 50)
alertas_recentes: list[dict] = []

# ---------------------------------------------------------------------------
# Dashboard HTML embutido
# ---------------------------------------------------------------------------

HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Monitoramento Agrícola</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: system-ui, -apple-system, sans-serif;
    background: #f0f4f0;
    color: #1a2e1a;
    min-height: 100vh;
  }

  /* ── Cabeçalho ── */
  header {
    background: #2d6a2d;
    color: #fff;
    padding: 1rem 1.5rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
  }
  header h1 { font-size: 1.3rem; flex: 1; }
  .badge-ativo {
    background: #4caf50;
    color: #fff;
    padding: .25rem .75rem;
    border-radius: 999px;
    font-size: .8rem;
    font-weight: 700;
    letter-spacing: .03em;
  }
  #total-leituras { font-size: .85rem; opacity: .9; }
  #indicador {
    font-size: .75rem;
    opacity: .75;
    transition: opacity .2s;
  }
  #indicador.pisca { opacity: 1; color: #a5f3a5; }

  /* ── Painel de botões ── */
  .painel-botoes {
    background: #fff;
    border-bottom: 1px solid #d8e8d8;
    padding: .75rem 1.5rem;
    display: flex;
    align-items: center;
    gap: .75rem;
    flex-wrap: wrap;
  }
  .painel-botoes span {
    font-size: .85rem;
    font-weight: 600;
    color: #444;
    margin-right: .25rem;
  }
  .btn {
    border: none;
    border-radius: 6px;
    padding: .45rem 1rem;
    font-size: .85rem;
    font-weight: 600;
    cursor: pointer;
    transition: filter .15s, transform .1s;
  }
  .btn:active { transform: scale(.96); }
  .btn:hover  { filter: brightness(.9); }
  .btn-infestacao  { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
  .btn-irrigacao   { background: #dbeafe; color: #1e40af; border: 1px solid #93c5fd; }
  .btn-temperatura { background: #fef9c3; color: #854d0e; border: 1px solid #fde047; }

  /* ── Contadores ── */
  .contadores {
    display: flex;
    gap: .75rem;
    padding: .75rem 1.5rem;
    flex-wrap: wrap;
  }
  .contador {
    background: #fff;
    border-radius: 8px;
    padding: .5rem 1rem;
    font-size: .8rem;
    font-weight: 700;
    border-left: 4px solid;
    box-shadow: 0 1px 3px rgba(0,0,0,.06);
  }
  .contador.critico { border-color: #ef4444; color: #991b1b; }
  .contador.atencao { border-color: #f59e0b; color: #92400e; }
  .contador.aviso   { border-color: #3b82f6; color: #1e40af; }

  /* ── Layout de duas colunas ── */
  .grid {
    display: grid;
    grid-template-columns: 1fr 1.6fr;
    gap: 1rem;
    padding: 0 1.5rem 1.5rem;
  }
  @media (max-width: 768px) {
    .grid { grid-template-columns: 1fr; }
  }

  /* ── Cabeçalho de coluna com botão Limpar ── */
  .coluna-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: .6rem;
    padding-bottom: .4rem;
    border-bottom: 2px solid #c8e6c9;
  }
  .coluna-header h2 {
    font-size: .95rem;
    font-weight: 700;
    color: #2d6a2d;
  }
  .coluna h2 {
    font-size: .95rem;
    font-weight: 700;
    color: #2d6a2d;
    margin-bottom: .6rem;
    padding-bottom: .4rem;
    border-bottom: 2px solid #c8e6c9;
  }
  .btn-limpar {
    background: #f1f5f1;
    color: #555;
    border: 1px solid #ccc;
    border-radius: 6px;
    padding: .3rem .75rem;
    font-size: .78rem;
    font-weight: 600;
    cursor: pointer;
    transition: background .15s;
  }
  .btn-limpar:hover { background: #e2e8e2; }

  /* ── Cards de alerta ── */
  #lista-alertas {
    display: flex;
    flex-direction: column;
    gap: .6rem;
    max-height: 560px;
    overflow-y: auto;
  }

  .card-alerta {
    border-radius: 8px;
    padding: .75rem 1rem;
    border-left: 5px solid;
    box-shadow: 0 1px 3px rgba(0,0,0,.07);
    font-size: .85rem;
    line-height: 1.5;
  }
  .card-alerta.critico { background: #fff1f1; border-color: #ef4444; }
  .card-alerta.atencao { background: #fffbeb; border-color: #f59e0b; }
  .card-alerta.aviso   { background: #eff6ff; border-color: #3b82f6; }

  .card-alerta .linha-topo {
    display: flex;
    align-items: center;
    gap: .5rem;
    margin-bottom: .2rem;
  }
  .card-alerta .tag-nivel {
    font-size: .72rem;
    font-weight: 800;
    letter-spacing: .04em;
    padding: .1rem .45rem;
    border-radius: 4px;
  }
  .critico .tag-nivel { background: #ef4444; color: #fff; }
  .atencao .tag-nivel { background: #f59e0b; color: #fff; }
  .aviso   .tag-nivel { background: #3b82f6; color: #fff; }

  .card-alerta .talhao  { font-weight: 700; }
  .card-alerta .detalhe { color: #444; }
  .card-alerta .ts      { font-size: .75rem; color: #888; margin-top: .15rem; }

  .card-alerta .card-footer {
    margin-top: .55rem;
    display: flex;
    justify-content: flex-end;
  }
  .btn-detalhes {
    background: transparent;
    border: 1px solid currentColor;
    border-radius: 5px;
    padding: .25rem .65rem;
    font-size: .76rem;
    font-weight: 600;
    cursor: pointer;
    opacity: .75;
    transition: opacity .15s, background .15s;
  }
  .critico .btn-detalhes { color: #991b1b; }
  .atencao .btn-detalhes { color: #92400e; }
  .aviso   .btn-detalhes { color: #1e40af; }
  .btn-detalhes:hover { opacity: 1; background: rgba(0,0,0,.05); }

  .sem-alertas {
    background: #f0fff4;
    border: 1px solid #a7f3d0;
    border-radius: 8px;
    padding: 1rem;
    font-size: .9rem;
    color: #065f46;
  }

  /* ── Tabela de leituras ── */
  .tabela-wrap { overflow-x: auto; max-height: 560px; overflow-y: auto; }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: .78rem;
    background: #fff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,.06);
  }
  thead { background: #2d6a2d; color: #fff; position: sticky; top: 0; }
  th { padding: .5rem .6rem; text-align: left; font-weight: 600; white-space: nowrap; }
  td { padding: .4rem .6rem; border-bottom: 1px solid #f0f0f0; white-space: nowrap; }
  tbody tr:nth-child(even) { background: #f7faf7; }
  tbody tr:hover { background: #e8f5e9; }

  .praga-badge {
    display: inline-block;
    padding: .1rem .4rem;
    border-radius: 4px;
    font-size: .72rem;
    font-weight: 600;
  }
  .praga-nenhuma               { background: #d1fae5; color: #065f46; }
  .praga-pulgao,
  .praga-lagarta,
  .praga-acaro                 { background: #fee2e2; color: #991b1b; }

  /* ── Modal overlay ── */
  #modal-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,.55);
    z-index: 1000;
    align-items: center;
    justify-content: center;
    padding: 1rem;
  }

  #modal-box {
    background: #fff;
    border-radius: 12px;
    width: 100%;
    max-width: 600px;
    max-height: 90vh;
    overflow-y: auto;
    box-shadow: 0 20px 60px rgba(0,0,0,.35);
    display: flex;
    flex-direction: column;
  }

  #modal-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    padding: 1.1rem 1.25rem .9rem;
    border-bottom: 1px solid #eee;
    gap: .75rem;
  }
  #modal-header-info { flex: 1; }
  #modal-titulo {
    font-size: 1.05rem;
    font-weight: 800;
    display: flex;
    align-items: center;
    gap: .45rem;
    flex-wrap: wrap;
    margin-bottom: .3rem;
  }
  #modal-tag-nivel {
    font-size: .72rem;
    font-weight: 800;
    letter-spacing: .04em;
    padding: .15rem .55rem;
    border-radius: 4px;
  }
  #modal-detalhe  { font-size: .88rem; color: #444; margin-bottom: .2rem; }
  #modal-ts       { font-size: .78rem; color: #888; }

  .btn-fechar-x {
    background: none;
    border: none;
    font-size: 1.3rem;
    cursor: pointer;
    color: #888;
    line-height: 1;
    padding: .1rem .3rem;
    border-radius: 4px;
    transition: color .15s, background .15s;
    flex-shrink: 0;
  }
  .btn-fechar-x:hover { color: #222; background: #f0f0f0; }

  #modal-body { padding: 1rem 1.25rem; flex: 1; }

  .modal-secao {
    margin-bottom: 1.1rem;
  }
  .modal-secao:last-child { margin-bottom: 0; }

  .modal-secao h3 {
    font-size: .8rem;
    font-weight: 800;
    letter-spacing: .06em;
    text-transform: uppercase;
    color: #555;
    margin-bottom: .55rem;
    display: flex;
    align-items: center;
    gap: .4rem;
  }

  .modal-secao ol,
  .modal-secao ul {
    padding-left: 1.3rem;
    display: flex;
    flex-direction: column;
    gap: .35rem;
  }
  .modal-secao li { font-size: .87rem; line-height: 1.5; color: #333; }

  .lista-acoes li::marker { content: "✓  "; color: #16a34a; font-weight: 700; }
  .lista-acoes li { color: #166534; }

  #modal-footer {
    display: flex;
    justify-content: flex-end;
    gap: .65rem;
    padding: .85rem 1.25rem;
    border-top: 1px solid #eee;
    background: #fafafa;
    border-radius: 0 0 12px 12px;
  }

  .btn-relatorio {
    background: #2d6a2d;
    color: #fff;
    border: none;
    border-radius: 6px;
    padding: .5rem 1.1rem;
    font-size: .85rem;
    font-weight: 700;
    cursor: pointer;
    transition: background .15s;
  }
  .btn-relatorio:hover { background: #1e4d1e; }

  .btn-fechar-modal {
    background: #f1f5f1;
    color: #444;
    border: 1px solid #ccc;
    border-radius: 6px;
    padding: .5rem 1.1rem;
    font-size: .85rem;
    font-weight: 600;
    cursor: pointer;
    transition: background .15s;
  }
  .btn-fechar-modal:hover { background: #e2e8e2; }

  /* ── Separador de seções do modal ── */
  .modal-divisor {
    border: none;
    border-top: 1px solid #eee;
    margin: .75rem 0 1rem;
  }
</style>
</head>
<body>

<!-- ── Cabeçalho ─────────────────────────────────────────────────────────── -->
<header>
  <h1>🌱 Monitoramento Agrícola em Tempo Real</h1>
  <span class="badge-ativo">● SISTEMA ATIVO</span>
  <span id="total-leituras">— leituras no CSV</span>
  <span id="indicador">● atualizado —</span>
</header>

<!-- ── Botões de disparo manual ──────────────────────────────────────────── -->
<div class="painel-botoes">
  <span>DISPARAR ALERTA MANUAL:</span>
  <button class="btn btn-infestacao"  onclick="forcar('infestacao')">🚨 Forçar Infestação</button>
  <button class="btn btn-irrigacao"   onclick="forcar('irrigacao')">💧 Forçar Irrigação</button>
  <button class="btn btn-temperatura" onclick="forcar('temperatura')">🌡 Forçar Temperatura</button>
</div>

<!-- ── Contadores ─────────────────────────────────────────────────────────── -->
<div class="contadores">
  <div class="contador critico">🚨 CRÍTICO: <span id="cnt-critico">0</span></div>
  <div class="contador atencao">⚠️ ATENÇÃO: <span id="cnt-atencao">0</span></div>
  <div class="contador aviso">🔵 AVISO: <span id="cnt-aviso">0</span></div>
</div>

<!-- ── Grid principal ────────────────────────────────────────────────────── -->
<div class="grid">

  <div class="coluna">
    <div class="coluna-header">
      <h2>ALERTAS ATIVOS (<span id="num-alertas">0</span>)</h2>
      <button class="btn-limpar" onclick="limparAlertas()">🗑 Limpar</button>
    </div>
    <div id="lista-alertas">
      <div class="sem-alertas">✅ Nenhum alerta no momento — sistema operando normalmente</div>
    </div>
  </div>

  <div class="coluna">
    <h2>ÚLTIMAS LEITURAS</h2>
    <div class="tabela-wrap">
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Talhão</th>
            <th>Folhas %</th>
            <th>Praga</th>
            <th>Umidade %</th>
            <th>Temp °C</th>
            <th>Irrigação</th>
          </tr>
        </thead>
        <tbody id="tbody-leituras">
          <tr><td colspan="7" style="text-align:center;color:#aaa;padding:1rem">Carregando...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</div>

<!-- ── Modal de detalhes do alerta ───────────────────────────────────────── -->
<div id="modal-overlay" onclick="fecharPorOverlay(event)">
  <div id="modal-box">

    <div id="modal-header">
      <div id="modal-header-info">
        <div id="modal-titulo">
          <span id="modal-icone"></span>
          <span id="modal-tag-nivel"></span>
          <span id="modal-regra"></span>
          &mdash;
          <span>Talhão <span id="modal-talhao"></span></span>
        </div>
        <div id="modal-detalhe"></div>
        <div id="modal-ts"></div>
      </div>
      <button class="btn-fechar-x" onclick="fecharModal()" title="Fechar">✕</button>
    </div>

    <div id="modal-body">
      <div class="modal-secao">
        <h3>📋 Recomendações de Ação</h3>
        <ol id="modal-recomendacoes"></ol>
      </div>

      <hr class="modal-divisor">

      <div class="modal-secao">
        <h3>⚙️ Ações Automáticas Realizadas pelo Sistema</h3>
        <ul id="modal-acoes" class="lista-acoes"></ul>
      </div>
    </div>

    <div id="modal-footer">
      <button class="btn-relatorio" onclick="baixarRelatorio()">📄 Baixar Relatório</button>
      <button class="btn-fechar-modal" onclick="fecharModal()">✕ Fechar</button>
    </div>

  </div>
</div>

<script>
  let alertaAtual  = null;   // alerta aberto no modal no momento
  let alertasCache = [];    // cópia da última lista recebida de /api/dados

  // ── Utilitários ─────────────────────────────────────────────────────────

  function nivelClass(nivel) {
    const map = { 'CRÍTICO': 'critico', 'ATENÇÃO': 'atencao', 'AVISO': 'aviso' };
    return map[nivel] || 'aviso';
  }

  function pragaClass(praga) {
    if (praga === 'nenhuma') return 'praga-nenhuma';
    return 'praga-' + praga.replace(/ã/g,'a').replace(/á/g,'a').replace(/â/g,'a');
  }

  function horaAtual() {
    return new Date().toLocaleTimeString('pt-BR');
  }

  // ── Modal ────────────────────────────────────────────────────────────────

  function abrirModal(indice) {
    const alerta = alertasCache[indice];
    if (!alerta) return;
    alertaAtual = alerta;
    const cls = nivelClass(alerta.nivel);

    // Cabeçalho
    document.getElementById('modal-icone').textContent  = alerta.icone;
    document.getElementById('modal-regra').textContent  = alerta.regra;
    document.getElementById('modal-talhao').textContent = alerta.talhao_id;
    document.getElementById('modal-detalhe').textContent = alerta.detalhe;
    document.getElementById('modal-ts').textContent     = 'Timestamp: ' + alerta.timestamp;

    const tagNivel = document.getElementById('modal-tag-nivel');
    tagNivel.textContent = alerta.nivel;
    tagNivel.style.cssText = '';   // reset
    const tagColors = {
      critico: 'background:#ef4444;color:#fff',
      atencao: 'background:#f59e0b;color:#fff',
      aviso:   'background:#3b82f6;color:#fff'
    };
    tagNivel.style.cssText = (tagColors[cls] || '') +
      ';font-size:.72rem;font-weight:800;letter-spacing:.04em;padding:.15rem .55rem;border-radius:4px;';

    // Recomendações
    const olRec = document.getElementById('modal-recomendacoes');
    olRec.innerHTML = (alerta.recomendacoes || [])
      .map(r => '<li>' + r + '</li>').join('');

    // Ações automáticas
    const ulAcoes = document.getElementById('modal-acoes');
    ulAcoes.innerHTML = (alerta.acoes_automaticas || [])
      .map(a => '<li>' + a + '</li>').join('');

    document.getElementById('modal-overlay').style.display = 'flex';
  }

  function fecharModal() {
    document.getElementById('modal-overlay').style.display = 'none';
    alertaAtual = null;
  }

  // Fecha apenas se o clique foi diretamente no overlay (não no modal-box)
  function fecharPorOverlay(event) {
    if (event.target === document.getElementById('modal-overlay')) fecharModal();
  }

  // Fecha modal com tecla Escape
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') fecharModal();
  });

  // ── Download de relatório ────────────────────────────────────────────────

  function baixarRelatorio() {
    if (!alertaAtual) return;
    const a    = alertaAtual;
    const agora = new Date().toLocaleString('pt-BR');
    const cls   = nivelClass(a.nivel);

    const cores = {
      critico: { borda: '#ef4444', fundo: '#fff1f1', nivel: '#ef4444', texto: '#991b1b' },
      atencao: { borda: '#f59e0b', fundo: '#fffbeb', nivel: '#f59e0b', texto: '#92400e' },
      aviso:   { borda: '#3b82f6', fundo: '#eff6ff', nivel: '#3b82f6', texto: '#1e40af' },
    };
    const c = cores[cls] || cores.aviso;

    const liRec = (a.recomendacoes || [])
      .map(r => '<li>' + r + '</li>').join('');
    const liAcoes = (a.acoes_automaticas || [])
      .map(ac => '<li>&#10003; ' + ac + '</li>').join('');

    const html = '<!DOCTYPE html>\\n' +
'<html lang="pt-BR">\\n' +
'<head>\\n' +
'<meta charset="UTF-8">\\n' +
'<meta name="viewport" content="width=device-width, initial-scale=1.0">\\n' +
'<title>Relatório de Alerta — Talhão ' + a.talhao_id + '</title>\\n' +
'<style>\\n' +
'  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }\\n' +
'  body { font-family: system-ui, -apple-system, sans-serif; background: #f0f4f0; color: #1a2e1a; padding: 2rem 1rem; }\\n' +
'  .page { max-width: 720px; margin: 0 auto; box-shadow: 0 4px 24px rgba(0,0,0,.12); border-radius: 12px; overflow: hidden; }\\n' +
'  .rel-header { background: #2d6a2d; color: #fff; padding: 1.5rem 2rem; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: .75rem; }\\n' +
'  .rel-header h1 { font-size: 1.15rem; font-weight: 800; margin-bottom: .2rem; }\\n' +
'  .rel-header p  { font-size: .82rem; opacity: .8; }\\n' +
'  .rel-header-right { text-align: right; font-size: .8rem; opacity: .8; }\\n' +
'  .rel-header-right strong { display: block; font-size: .95rem; color: #fff; }\\n' +
'  .card-principal { background: ' + c.fundo + '; border-left: 6px solid ' + c.borda + '; padding: 1.25rem 2rem; border-bottom: 1px solid #e0e0e0; }\\n' +
'  .card-titulo { display: flex; align-items: center; gap: .65rem; flex-wrap: wrap; margin-bottom: .5rem; }\\n' +
'  .tag-nivel { background: ' + c.nivel + '; color: #fff; font-size: .72rem; font-weight: 800; letter-spacing: .06em; padding: .2rem .65rem; border-radius: 4px; }\\n' +
'  .card-titulo h2 { font-size: 1.05rem; color: ' + c.texto + '; font-weight: 800; }\\n' +
'  .card-detalhe { font-size: .92rem; color: #444; margin-bottom: .3rem; }\\n' +
'  .card-ts { font-size: .78rem; color: #888; }\\n' +
'  .meta-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: .5rem; padding: 1.1rem 2rem; background: #fff; border-bottom: 1px solid #e8e8e8; }\\n' +
'  .meta-item .label { color: #888; font-weight: 600; font-size: .72rem; text-transform: uppercase; letter-spacing: .05em; }\\n' +
'  .meta-item .valor { color: #1a2e1a; font-weight: 700; margin-top: .1rem; font-size: .88rem; }\\n' +
'  .secao { background: #fff; padding: 1.25rem 2rem; border-bottom: 1px solid #eee; }\\n' +
'  .secao:last-of-type { border-bottom: none; }\\n' +
'  .secao h3 { font-size: .78rem; font-weight: 800; text-transform: uppercase; letter-spacing: .07em; color: #555; margin-bottom: .85rem; padding-bottom: .45rem; border-bottom: 2px solid #e8e8e8; }\\n' +
'  .secao ol, .secao ul { padding-left: 0; list-style: none; display: flex; flex-direction: column; gap: .5rem; }\\n' +
'  .secao li { font-size: .9rem; line-height: 1.55; color: #333; padding: .55rem .8rem; background: #f8f8f8; border-radius: 6px; border-left: 3px solid #ddd; }\\n' +
'  .secao-rec li   { border-color: ' + c.borda + '; }\\n' +
'  .secao-acoes li { border-color: #16a34a; background: #f0fdf4; color: #14532d; font-weight: 500; }\\n' +
'  .rodape { background: #fff; padding: .9rem 2rem; text-align: center; font-size: .75rem; color: #999; border-top: 1px solid #eee; }\\n' +
'  @media print { body { background: #fff; padding: 0; } .page { box-shadow: none; border-radius: 0; } }\\n' +
'</style>\\n' +
'</head>\\n' +
'<body>\\n' +
'<div class="page">\\n' +
'  <div class="rel-header">\\n' +
'    <div><h1>&#127807; Relatório de Alerta Agrícola</h1><p>Sistema de Monitoramento Agrícola — gerado automaticamente</p></div>\\n' +
'    <div class="rel-header-right">Gerado em<strong>' + agora + '</strong></div>\\n' +
'  </div>\\n' +
'  <div class="card-principal">\\n' +
'    <div class="card-titulo">\\n' +
'      <span style="font-size:1.4rem">' + a.icone + '</span>\\n' +
'      <span class="tag-nivel">' + a.nivel + '</span>\\n' +
'      <h2>' + a.regra + ' — Talhão ' + a.talhao_id + '</h2>\\n' +
'    </div>\\n' +
'    <div class="card-detalhe">' + a.detalhe + '</div>\\n' +
'    <div class="card-ts">Leitura registrada em: ' + a.timestamp + '</div>\\n' +
'  </div>\\n' +
'  <div class="meta-grid">\\n' +
'    <div class="meta-item"><div class="label">Regra disparada</div><div class="valor">' + a.regra + '</div></div>\\n' +
'    <div class="meta-item"><div class="label">Nível</div><div class="valor" style="color:' + c.nivel + '">' + a.nivel + '</div></div>\\n' +
'    <div class="meta-item"><div class="label">Talhão</div><div class="valor">' + a.talhao_id + '</div></div>\\n' +
'    <div class="meta-item"><div class="label">Timestamp</div><div class="valor">' + a.timestamp + '</div></div>\\n' +
'  </div>\\n' +
'  <div class="secao secao-rec"><h3>&#128203; Recomendações de Ação</h3><ol>' + liRec + '</ol></div>\\n' +
'  <div class="secao secao-acoes"><h3>&#9881;&#65039; Ações Automáticas Realizadas pelo Sistema</h3><ul>' + liAcoes + '</ul></div>\\n' +
'  <div class="rodape">Este relatório foi gerado automaticamente pelo Sistema de Monitoramento Agrícola.</div>\\n' +
'</div>\\n' +
'</body>\\n' +
'</html>';

    const blob = new Blob([html], { type: 'text/html;charset=utf-8' });
    const url  = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href     = url;
    link.download = 'alerta_talhao' + a.talhao_id + '_' +
                    a.timestamp.replace(/[:.T]/g, '-') + '.html';
    link.click();
    URL.revokeObjectURL(url);
  }

  // ── Limpar alertas ───────────────────────────────────────────────────────

  async function limparAlertas() {
    await fetch('/api/limpar-alertas', { method: 'POST' });
    await atualizarDashboard();
  }

  // ── Render alertas ───────────────────────────────────────────────────────

  function renderAlertas(alertas) {
    alertasCache = alertas;   // salva para abrirModal(indice) usar
    const lista = document.getElementById('lista-alertas');
    document.getElementById('num-alertas').textContent = alertas.length;

    if (!alertas.length) {
      lista.innerHTML = '<div class="sem-alertas">✅ Nenhum alerta no momento — sistema operando normalmente</div>';
      return;
    }

    lista.innerHTML = alertas.map(function(a, i) {
      const cls = nivelClass(a.nivel);
      return '<div class="card-alerta ' + cls + '">' +
               '<div class="linha-topo">' +
                 '<span>' + a.icone + '</span>' +
                 '<span class="tag-nivel">' + a.nivel + '</span>' +
                 '<span class="talhao">Talhão ' + a.talhao_id + '</span>' +
                 '<span>&mdash; ' + a.regra + '</span>' +
               '</div>' +
               '<div class="detalhe">' + a.detalhe + '</div>' +
               '<div class="ts">' + a.timestamp + '</div>' +
               '<div class="card-footer">' +
                 '<button class="btn-detalhes" onclick="abrirModal(' + i + ')">' +
                   '🔍 Ver Detalhes' +
                 '</button>' +
               '</div>' +
             '</div>';
    }).join('');
  }

  // ── Render tabela ────────────────────────────────────────────────────────

  function renderLeituras(leituras) {
    const tbody = document.getElementById('tbody-leituras');
    if (!leituras.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#aaa">Sem dados</td></tr>';
      return;
    }
    tbody.innerHTML = leituras.map(function(l) {
      return '<tr>' +
        '<td>' + l.timestamp + '</td>' +
        '<td style="font-weight:700;text-align:center">' + l.talhao_id + '</td>' +
        '<td>' + parseFloat(l.perc_folhas_doentes).toFixed(1) + '%</td>' +
        '<td><span class="praga-badge ' + pragaClass(l.praga_detectada) + '">' + l.praga_detectada + '</span></td>' +
        '<td>' + parseFloat(l.umidade_solo_pct).toFixed(1) + '%</td>' +
        '<td>' + parseFloat(l.temperatura_c).toFixed(1) + '°C</td>' +
        '<td>' + l.nivel_irrigacao + '</td>' +
      '</tr>';
    }).join('');
  }

  // ── Atualização principal ────────────────────────────────────────────────

  async function atualizarDashboard() {
    try {
      const resp  = await fetch('/api/dados');
      const dados = await resp.json();

      renderAlertas(dados.alertas);
      renderLeituras(dados.leituras);

      document.getElementById('total-leituras').textContent =
        dados.totais.leituras_csv + ' leituras no CSV';
      document.getElementById('cnt-critico').textContent = dados.totais.critico;
      document.getElementById('cnt-atencao').textContent = dados.totais.atencao;
      document.getElementById('cnt-aviso').textContent   = dados.totais.aviso;

      const ind = document.getElementById('indicador');
      ind.textContent = '● atualizado às ' + horaAtual();
      ind.classList.add('pisca');
      setTimeout(function() { ind.classList.remove('pisca'); }, 600);

    } catch (e) {
      console.error('Erro ao buscar dados:', e);
    }
  }

  // ── Forçar alerta manual ─────────────────────────────────────────────────

  async function forcar(tipo) {
    try {
      await fetch('/api/gerar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tipo: tipo })
      });
      await atualizarDashboard();
    } catch (e) {
      console.error('Erro ao forçar alerta:', e);
    }
  }

  // ── Loop de atualização automática ──────────────────────────────────────

  atualizarDashboard();
  setInterval(atualizarDashboard, 3000);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Thread de geração contínua
# ---------------------------------------------------------------------------

def loop_geracao() -> None:
    """
    Daemon thread: a cada 5 segundos gera uma leitura normal,
    persiste no CSV e reavalia todas as regras sobre as últimas 50 linhas.
    """
    while True:
        leitura = gerar_leitura_normal()
        salvar_no_csv(leitura, CSV_PATH)

        with csv_lock:
            df = pd.read_csv(CSV_PATH).tail(50)

        novos_alertas = motor.avaliar(df)
        alertas_recentes[:] = novos_alertas[:50]  # atualiza in-place, máx 50

        time.sleep(5)


# ---------------------------------------------------------------------------
# Rotas Flask
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve o dashboard HTML embutido."""
    return render_template_string(HTML)


@app.route("/api/dados")
def api_dados():
    """Retorna as últimas 20 leituras do CSV e os alertas atuais em JSON."""
    try:
        with csv_lock:
            df = pd.read_csv(CSV_PATH)
        total_linhas = len(df)
        ultimas = df.tail(20).iloc[::-1]  # mais recentes primeiro
        leituras_json = ultimas.to_dict(orient="records")
    except FileNotFoundError:
        total_linhas = 0
        leituras_json = []

    contagem = {
        "leituras_csv": total_linhas,
        "critico": sum(1 for a in alertas_recentes if a["nivel"] == "CRÍTICO"),
        "atencao": sum(1 for a in alertas_recentes if a["nivel"] == "ATENÇÃO"),
        "aviso":   sum(1 for a in alertas_recentes if a["nivel"] == "AVISO"),
    }

    return jsonify(
        leituras=leituras_json,
        alertas=alertas_recentes,
        totais=contagem,
    )


@app.route("/api/gerar", methods=["POST"])
def api_gerar():
    """
    Gera uma leitura forçada para o tipo de alerta informado.
    Body JSON: { "tipo": "infestacao" | "irrigacao" | "temperatura" }
    """
    dados = request.get_json(force=True, silent=True) or {}
    tipo = dados.get("tipo", "infestacao")

    leitura = gerar_leitura_forcada(tipo)
    salvar_no_csv(leitura, CSV_PATH)

    with csv_lock:
        df = pd.read_csv(CSV_PATH).tail(50)

    novos_alertas = motor.avaliar(df)
    alertas_recentes[:] = novos_alertas[:50]

    return jsonify(ok=True, leitura=leitura)


@app.route("/api/limpar-alertas", methods=["POST"])
def api_limpar_alertas():
    """Remove todos os alertas da lista em memória sem apagar o CSV."""
    alertas_recentes.clear()
    return jsonify(ok=True)


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("🌱 Iniciando sistema de monitoramento agrícola...")
    print("📊 Dashboard disponível em: http://localhost:5000")
    print("⏱  Gerando dados a cada 5 segundos automaticamente")

    t = threading.Thread(target=loop_geracao, daemon=True)
    t.start()

    app.run(debug=False, port=5000)
