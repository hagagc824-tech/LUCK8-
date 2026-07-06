const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

// Cấu hình chạy Multi-threading Worker nếu không phải luồng chính
if (!isMainThread) {
  // Luồng con xử lý thuật toán phân tích logic nâng cao (Không Random)
  const { data, type, learningStats, patternWeights } = workerData;
  const result = executeLogicAnalysis(data, type, learningStats, patternWeights);
  parentPort.postMessage(result);
  process.exit(0);
}

const app = express();
const PORT = process.env.PORT || 5000;
const API_URL_HU = 'https://luck8bot.com/api/GetNewLottery/Taixiu';
const API_URL_MD5 = 'https://luck8bot.com/api/GetNewLottery/TaixiuMd5';

const LEARNING_FILE = 'learning_data.json';
const HISTORY_FILE = 'prediction_history.json';

let predictionHistory = { hu: [], md5: [] };
const MAX_HISTORY = 100;
const AUTO_SAVE_INTERVAL = 30000;
let lastProcessedPhien = { hu: null, md5: null };

let learningData = {
  hu: { predictions: [], patternStats: {}, totalPredictions: 0, correctPredictions: 0, patternWeights: {}, lastUpdate: null, streakAnalysis: { wins: 0, losses: 0, currentStreak: 0, bestStreak: 0, worstStreak: 0 }, adaptiveThresholds: {}, recentAccuracy: [] },
  md5: { predictions: [], patternStats: {}, totalPredictions: 0, correctPredictions: 0, patternWeights: {}, lastUpdate: null, streakAnalysis: { wins: 0, losses: 0, currentStreak: 0, bestStreak: 0, worstStreak: 0 }, adaptiveThresholds: {}, recentAccuracy: [] }
};

const DEFAULT_PATTERN_WEIGHTS = {
  'cau_bet': 1.0, 'cau_dao_11': 1.0, 'cau_22': 1.0, 'cau_33': 1.0, 'cau_121': 1.0,
  'cau_123': 1.0, 'cau_321': 1.0, 'cau_nhay_coc': 1.0, 'cau_nhip_nghieng': 1.0,
  'cau_3van1': 1.0, 'cau_be_cau': 1.0, 'cau_chu_ky': 1.0, 'distribution': 1.0,
  'dice_pattern': 1.0, 'sum_trend': 1.0, 'edge_cases': 1.0, 'momentum': 1.0,
  'cau_tu_nhien': 1.0, 'dice_trend_line': 1.0, 'dice_trend_line_md5': 1.0,
  'break_pattern_hu': 1.0, 'break_pattern_md5': 1.0, 'fibonacci': 1.0,
  'resistance_support': 1.0, 'wave': 1.0, 'golden_ratio': 1.0, 'day_gay': 1.0,
  'day_gay_md5': 1.0, 'cau_44': 1.0, 'cau_55': 1.0, 'cau_212': 1.0,
  'cau_1221': 1.0, 'cau_2112': 1.0, 'cau_gap': 1.0, 'cau_ziczac': 1.0,
  'cau_doi': 1.0, 'cau_rong': 1.0, 'smart_bet': 1.0, 'break_pattern_advanced': 1.0,
  'break_streak': 1.0, 'alternating_break': 1.0, 'double_pair_break': 1.0, 'triple_pattern': 1.0
};

let historicalDataCache = { hu: [], md5: [] };

// ==================== HÀM ĐA LUỒNG WORKER LAUNCHER ====================
function runAnalysisWorker(data, type) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(__filename, {
      workerData: {
        data,
        type,
        learningStats: {
          totalPredictions: learningData[type].totalPredictions,
          correctPredictions: learningData[type].correctPredictions,
          streakAnalysis: learningData[type].streakAnalysis,
          recentAccuracy: learningData[type].recentAccuracy,
          patternStats: learningData[type].patternStats
        },
        patternWeights: learningData[type].patternWeights
      }
    });
    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}

// ==================== CORE ALGORITHM (CHẠY TRÊN LUỒNG CON THUẦN LOGIC) ====================
function executeLogicAnalysis(data, type, learningStats, patternWeights) {
  const last50 = data.slice(0, 50);
  const results = last50.map(d => d.Ket_qua);
  
  const weights = patternWeights && Object.keys(patternWeights).length > 0 ? patternWeights : { ...DEFAULT_PATTERN_WEIGHTS };
  const stats = learningStats.patternStats || {};

  function getWeight(pId) { return weights[pId] || 1.0; }

  // Các hàm phân tích toán học & logic cầu
  const cauBet = (() => {
    if (results.length < 3) return { detected: false };
    let sType = results[0], sLen = 1;
    for (let i = 1; i < results.length; i++) { if (results[i] === sType) sLen++; else break; }
    if (sLen >= 3) {
      const w = getWeight('cau_bet');
      let shouldBreak = sLen >= 6;
      const pStats = stats['cau_bet'];
      if (pStats && pStats.recentResults && pStats.recentResults.length >= 5) {
        const recentAcc = pStats.recentResults.reduce((a, b) => a + b, 0) / pStats.recentResults.length;
        if (recentAcc < 0.4) shouldBreak = !shouldBreak;
      }
      return { detected: true, prediction: shouldBreak ? (sType === 'Tài' ? 'Xỉu' : 'Tài') : sType, confidence: Math.round((shouldBreak ? Math.min(12, sLen * 2) : Math.min(15, sLen * 3)) * w), name: `Cầu Bệt ${sLen} phiên` };
    }
    return { detected: false };
  })();

  const cauDao11 = (() => {
    if (results.length < 4) return { detected: false };
    let altLen = 1;
    for (let i = 1; i < Math.min(results.length, 10); i++) { if (results[i] !== results[i - 1]) altLen++; else break; }
    if (altLen >= 4) { return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(Math.min(14, altLen * 2 + 4) * getWeight('cau_dao_11')), name: `Cầu Đảo 1-1 (${altLen} phiên)` }; }
    return { detected: false };
  })();

  const cau22 = (() => {
    if (results.length < 6) return { detected: false };
    let pairCount = 0, i = 0, pattern = [];
    while (i < results.length - 1 && pairCount < 4) { if (results[i] === results[i + 1]) { pattern.push(results[i]); pairCount++; i += 2; } else break; }
    if (pairCount >= 2) {
      let isAlt = true; for (let j = 1; j < pattern.length; j++) { if (pattern[j] === pattern[j - 1]) { isAlt = false; break; } }
      if (isAlt) { return { detected: true, prediction: pattern[pattern.length - 1] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(Math.min(12, pairCount * 3 + 3) * getWeight('cau_22')), name: `Cầu 2-2 (${pairCount} cặp)` }; }
    }
    return { detected: false };
  })();

  const cau33 = (() => {
    if (results.length < 6) return { detected: false };
    let tCount = 0, i = 0, pattern = [];
    while (i < results.length - 2) { if (results[i] === results[i + 1] && results[i + 1] === results[i + 2]) { pattern.push(results[i]); tCount++; i += 3; } else break; }
    if (tCount >= 1) {
      const pos = results.length % 3;
      return { detected: true, prediction: (pos === 0) ? (pattern[pattern.length - 1] === 'Tài' ? 'Xỉu' : 'Tài') : pattern[pattern.length - 1], confidence: Math.round(Math.min(13, tCount * 4 + 5) * getWeight('cau_33')), name: `Cầu 3-3 (${tCount} bộ ba)` };
    }
    return { detected: false };
  })();

  const cau121 = (() => {
    if (results.length < 4) return { detected: false };
    const p = results.slice(0, 4);
    if (p[0] !== p[1] && p[1] === p[2] && p[2] !== p[3] && p[0] === p[3]) return { detected: true, prediction: p[0], confidence: Math.round(10 * getWeight('cau_121')), name: 'Cầu 1-2-1' };
    return { detected: false };
  })();

  const cau123 = (() => {
    if (results.length < 6) return { detected: false };
    if (results[3] === results[4] && results[3] !== results[5] && results.slice(0, 3).every(r => r === results[0]) && results[0] !== results[3]) return { detected: true, prediction: results[5], confidence: Math.round(11 * getWeight('cau_123')), name: 'Cầu 1-2-3' };
    return { detected: false };
  })();

  const cau321 = (() => {
    if (results.length < 6) return { detected: false };
    if (results.slice(3, 6).every(r => r === results[3]) && results[1] === results[2] && results[3] !== results[1] && results[0] !== results[1]) return { detected: true, prediction: results[1], confidence: Math.round(12 * getWeight('cau_321')), name: 'Cầu 3-2-1' };
    return { detected: false };
  })();

  const cauNhayCoc = (() => {
    if (results.length < 6) return { detected: false };
    const skp = []; for (let i = 0; i < Math.min(results.length, 12); i += 2) skp.push(results[i]);
    if (skp.length >= 3) {
      if (skp.slice(0, 3).every(r => r === skp[0])) return { detected: true, prediction: skp[0], confidence: Math.round(8 * getWeight('cau_nhay_coc')), name: 'Cầu Nhảy Cóc' };
      let alt = true; for (let i = 1; i < skp.length - 1; i++) { if (skp[i] === skp[i - 1]) { alt = false; break; } }
      if (alt) return { detected: true, prediction: skp[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(7 * getWeight('cau_nhay_coc')), name: 'Cầu Nhảy Cóc Đảo' };
    }
    return { detected: false };
  })();

  const cauNhipNghieng = (() => {
    if (results.length < 5) return { detected: false };
    const t5 = results.slice(0, 5).filter(r => r === 'Tài').length;
    if (t5 >= 4) return { detected: true, prediction: 'Tài', confidence: Math.round(9 * getWeight('cau_nhip_nghieng')), name: `Cầu Nhịp Nghiêng 5 (${t5} Tài)` };
    if (t5 <= 1) return { detected: true, prediction: 'Xỉu', confidence: Math.round(9 * getWeight('cau_nhip_nghieng')), name: `Cầu Nhịp Nghiêng 5 (${5 - t5} Xỉu)` };
    return { detected: false };
  })();

  const cau3Van1 = (() => {
    if (results.length < 4) return { detected: false };
    const t4 = results.slice(0, 4).filter(r => r === 'Tài').length;
    if (t4 === 3) return { detected: true, prediction: 'Xỉu', confidence: Math.round(8 * getWeight('cau_3van1')), name: 'Cầu 3 Ván 1 (3T-1X)' };
    if (t4 === 1) return { detected: true, prediction: 'Tài', confidence: Math.round(8 * getWeight('cau_3van1')), name: 'Cầu 3 Ván 1 (3X-1T)' };
    return { detected: false };
  })();

  const cauBeCau = (() => {
    if (results.length < 8) return { detected: false };
    let sLen = 1; for (let i = 1; i < results.length; i++) { if (results[i] === results[0]) sLen++; else break; }
    if (sLen >= 4) {
      let prvLen = 1; const bfr = results.slice(sLen);
      for (let i = 1; i < bfr.length; i++) { if (bfr[i] === bfr[0]) prvLen++; else break; }
      if (prvLen >= 3 && bfr[0] !== results[0]) return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(11 * getWeight('cau_be_cau')), name: 'Cầu Bẻ Cầu' };
    }
    return { detected: false };
  })();

  const cau44 = (() => {
    if (results.length < 8) return { detected: false };
    if (results.slice(0, 4).every(r => r === results[0]) && results.slice(4, 8).every(r => r !== results[0])) return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(14 * getWeight('cau_44')), name: 'Cầu 4-4 Gốc' };
    return { detected: false };
  })();

  const cau55 = (() => {
    if (results.length < 10) return { detected: false };
    if (results.slice(0, 5).every(r => r === results[0]) && results.slice(5, 10).every(r => r !== results[0])) return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(15 * getWeight('cau_55')), name: 'Cầu 5-5 Gốc' };
    return { detected: false };
  })();

  const cau212 = (() => {
    if (results.length < 5) return { detected: false };
    const p = results.slice(0, 5);
    if (p[0] === p[1] && p[1] !== p[2] && p[2] === p[3] && p[3] === p[4] && p[0] !== p[2]) return { detected: true, prediction: p[0], confidence: Math.round(11 * getWeight('cau_212')), name: 'Cầu 2-1-2' };
    return { detected: false };
  })();

  const cau1221 = (() => {
    if (results.length < 6) return { detected: false };
    const p = results.slice(0, 6);
    if (p[0] !== p[1] && p[1] === p[2] && p[2] === p[3] && p[3] !== p[4] && p[4] === p[5] && p[0] !== p[1]) return { detected: true, prediction: p[0], confidence: Math.round(12 * getWeight('cau_1221')), name: 'Cầu 1-2-2-1' };
    return { detected: false };
  })();

  const cauZiczac = (() => {
    if (results.length < 5) return { detected: false };
    let zz = 0; for (let i = 0; i < results.length - 2; i++) { if (results[i] !== results[i + 1] && results[i] === results[i + 2]) zz++; else break; }
    if (zz >= 3) return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(Math.min(13, zz * 2 + 5) * getWeight('cau_ziczac')), name: `Cầu Ziczac (${zz} lần)` };
    return { detected: false };
  })();

  const cauRong = (() => {
    let sLen = 1; for (let i = 1; i < results.length; i++) { if (results[i] === results[0]) sLen++; else break; }
    if (sLen >= 6) return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(Math.min(16, sLen + 8) * getWeight('cau_rong')), name: `Cầu Rồng ${sLen} phiên` };
    return { detected: false };
  })();

  const breakPatternAdvanced = (() => {
    if (results.length < 6) return { detected: false };
    if (results[0] !== results[1] && results[1] !== results[2] && results[2] === results[3] && results[3] === results[4] && results[4] !== results[5]) return { detected: true, prediction: results[2] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round(14 * getWeight('break_pattern_advanced')), name: 'Cầu nâng cao 1-1-2-2-1' };
    return { detected: false };
  })();

  const breakStreak = (() => {
    let sLen = 1; for (let i = 1; i < results.length; i++) { if (results[i] === results[0]) sLen++; else break; }
    if (sLen >= 5) return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round((12 + Math.min(sLen, 8)) * getWeight('break_streak')), name: `Bẻ Chuỗi ${sLen} phiên` };
    return { detected: false };
  })();

  const alternatingBreak = (() => {
    let alt = 0; for (let i = 0; i < results.length - 1; i++) { if (results[i] !== results[i + 1]) alt++; else break; }
    if (alt >= 6) return { detected: true, prediction: alt >= 9 ? (results[0] === 'Tài' ? 'Xỉu' : 'Tài') : results[0], confidence: Math.round((13 + Math.min(alt, 10) - 5) * getWeight('alternating_break')), name: `Bẻ Cầu Đảo ${alt} Phiên` };
    return { detected: false };
  })();

  const doublePairBreak = (() => {
    if (results.length < 8) return { detected: false };
    if (results[0] === results[1] && results[2] === results[3] && results[4] === results[5] && results[6] === results[7]) {
      const alt = results[0] !== results[2] && results[2] !== results[4] && results[4] !== results[6];
      return { detected: true, prediction: results[0] === 'Tài' ? 'Xỉu' : 'Tài', confidence: Math.round((alt ? 14 : 16) * getWeight('double_pair_break')), name: alt ? 'Cặp Đảo Xen Kẽ' : '4 Cặp Trùng Bệt' };
    }
    return { detected: false };
  })();

  const triplePattern = (() => {
    if (results.length < 9) return { detected: false };
    if (results.slice(0, 3).every(r => r === results[0]) && results.slice(3, 6).every(r => r === results[3]) && results.slice(6, 9).every(r => r === results[6])) {
      const allSame = results[0] === results[3] && results[3] === results[6];
      return { detected: true, prediction: allSame ? (results[0] === 'Tài' ? 'Xỉu' : 'Tài') : results[0], confidence: Math.round((allSame ? 17 : 15) * getWeight('triple_pattern')), name: allSame ? '3 Bộ Ba Trùng' : 'Bộ Ba Đảo Đối' };
    }
    return { detected: false };
  })();

  const diceTrendLine = (() => {
    if (last50.length < 2) return { detected: false };
    const curD = [last50[0].Xuc_xac_1, last50[0].Xuc_xac_2, last50[0].Xuc_xac_3];
    const prvD = [last50[1].Xuc_xac_1, last50[1].Xuc_xac_2, last50[1].Xuc_xac_3];
    let up = 0, down = 0;
    for (let i = 0; i < 3; i++) { if (curD[i] > prvD[i]) up++; else if (curD[i] < prvD[i]) down++; }
    const pId = type === 'hu' ? 'dice_trend_line' : 'dice_trend_line_md5';
    if (up === 1 && down === 2) return { detected: true, prediction: 'Tài', confidence: Math.round(12 * getWeight(pId)), name: 'Biểu Đồ Đường (1 lên 2 xuống)' };
    if (up === 2 && down === 1) return { detected: true, prediction: 'Xỉu', confidence: Math.round(12 * getWeight(pId)), name: 'Biểu Đồ Đường (2 lên 1 xuống)' };
    return { detected: false };
  })();

  const dayGay = (() => {
    if (last50.length < 2) return { detected: false };
    const curD = [last50[0].Xuc_xac_1, last50[0].Xuc_xac_2, last50[0].Xuc_xac_3];
    const prvD = [last50[1].Xuc_xac_1, last50[1].Xuc_xac_2, last50[1].Xuc_xac_3];
    let same = 0, up = 0, down = 0;
    for (let i = 0; i < 3; i++) { if (curD[i] === prvD[i]) same++; else if (curD[i] > prvD[i]) up++; else down++; }
    const pId = type === 'hu' ? 'day_gay' : 'day_gay_md5';
    if (same === 2 && up === 1) return { detected: true, prediction: 'Xỉu', confidence: Math.round(14 * getWeight(pId)), name: 'Dây Gãy (2 Ngang + 1 Lên)' };
    if (same === 2 && down === 1) return { detected: true, prediction: 'Tài', confidence: Math.round(14 * getWeight(pId)), name: 'Dây Gãy (2 Ngang + 1 Xuống)' };
    return { detected: false };
  })();

  let predictions = [], factors = [];
  const allAlgos = [
    cauBet, cauDao11, cau22, cau33, cau121, cau123, cau321, cauNhayCoc, cauNhipNghieng,
    cau3Van1, cauBeCau, cau44, cau55, cau212, cau1221, cauZiczac, cauRong,
    breakPatternAdvanced, breakStreak, alternatingBreak, doublePairBreak, triplePattern,
    diceTrendLine, dayGay
  ];

  allAlgos.forEach(a => {
    if (a.detected) {
      predictions.push({ prediction: a.prediction, confidence: a.confidence });
      factors.push(a.name);
    }
  });

  if (predictions.length === 0) {
    predictions.push({ prediction: results[0] || 'Tài', confidence: Math.round(5 * getWeight('cau_tu_nhien')) });
    factors.push('Cầu Tự Nhiên (Theo ván trước)');
  }

  const taiVotes = predictions.filter(p => p.prediction === 'Tài');
  const xiuVotes = predictions.filter(p => p.prediction === 'Xỉu');
  const taiScore = taiVotes.reduce((sum, p) => sum + p.confidence, 0);
  const xiuScore = xiuVotes.reduce((sum, p) => sum + p.confidence, 0);

  let finalPrediction = taiScore >= xiuScore ? 'Tài' : 'Xỉu';
  
  let totalScore = taiScore + xiuScore;
  let baseConfidence = totalScore > 0 ? Math.round((Math.max(taiScore, xiuScore) / totalScore) * 35) + 50 : 65;
  
  if (learningStats.recentAccuracy && learningStats.recentAccuracy.length >= 10) {
    const acc = learningStats.recentAccuracy.reduce((a, b) => a + b, 0) / learningStats.recentAccuracy.length;
    if (acc > 0.65) baseConfidence += 5;
    if (acc < 0.45) baseConfidence -= 4;
  }

  let finalConfidence = Math.max(50, Math.min(95, baseConfidence));

  return { prediction: finalPrediction, confidence: finalConfidence, factors };
}

// ==================== CÁC HÀM QUẢN LÝ DỮ LIỆU FILE HỆ THỐNG ====================
function loadLearningData() {
  try {
    if (fs.existsSync(LEARNING_FILE)) {
      learningData = { ...learningData, ...JSON.parse(fs.readFileSync(LEARNING_FILE, 'utf8')) };
    }
  } catch (e) { console.error('Error loading learning data:', e.message); }
}

function saveLearningData() {
  try { fs.writeFileSync(LEARNING_FILE, JSON.stringify(learningData, null, 2)); } catch (e) { console.error('Error saving learning data:', e.message); }
}

function loadPredictionHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
      predictionHistory = parsed.history || { hu: [], md5: [] };
      lastProcessedPhien = parsed.lastProcessedPhien || { hu: null, md5: null };
    }
  } catch (e) { console.error('Error loading history:', e.message); }
}

function savePredictionHistory() {
  try { fs.writeFileSync(HISTORY_FILE, JSON.stringify({ history: predictionHistory, lastProcessedPhien, lastSaved: new Date().toISOString() }, null, 2)); } catch (e) { console.error('Error saving history:', e.message); }
}

function transformApiData(apiData) {
  if (!apiData || apiData.state !== 1 || !apiData.data) return null;
  const item = apiData.data;
  const parts = item.OpenCode.split(',').map(num => parseInt(num.trim()));
  if (parts.length !== 3) return null;
  const tong = parts[0] + parts[1] + parts[2];
  return [{ Phien: parseInt(item.Expect), Ket_qua: tong >= 11 ? 'Tài' : 'Xỉu', Xuc_xac_1: parts[0], Xuc_xac_2: parts[1], Xuc_xac_3: parts[2], Tong: tong }];
}

async function fetchData(url, cacheKey) {
  try {
    const response = await axios.get(url);
    const transformed = transformApiData(response.data);
    if (transformed && transformed.length > 0) {
      const idx = historicalDataCache[cacheKey].findIndex(item => item.Phien === transformed[0].Phien);
      if (idx === -1) {
        historicalDataCache[cacheKey].unshift(transformed[0]);
        if (historicalDataCache[cacheKey].length > 60) historicalDataCache[cacheKey] = historicalDataCache[cacheKey].slice(0, 60);
      }
    }
    return historicalDataCache[cacheKey];
  } catch (error) {
    console.error(`Error fetching ${cacheKey} data:`, error.message);
    return historicalDataCache[cacheKey];
  }
}

function getPatternIdFromName(name) {
  const mapping = { 'Cầu Bệt': 'cau_bet', 'Cầu Đảo 1-1': 'cau_dao_11', 'Cầu 2-2': 'cau_22', 'Cầu 3-3': 'cau_33', 'Cầu 4-4': 'cau_44', 'Cầu 5-5': 'cau_55', 'Cầu 1-2-1': 'cau_121', 'Cầu 1-2-3': 'cau_123', 'Cầu 3-2-1': 'cau_321', 'Cầu Nhảy Cóc': 'cau_nhay_coc', 'Cầu Nhịp Nghiêng': 'cau_nhip_nghieng', 'Cầu 3 Ván 1': 'cau_3van1', 'Cầu Bẻ Cầu': 'cau_be_cau', 'Cầu Ziczac': 'cau_ziczac', 'Cầu Rồng': 'cau_rong', 'Biểu Đồ Đường': 'dice_trend_line', 'Dây Gãy': 'day_gay' };
  for (const [key, value] of Object.entries(mapping)) { if (name.includes(key)) return value; }
  return 'cau_tu_nhien';
}

function updateStatsAndWeights(type, patternId, isCorrect) {
  if (!learningData[type].patternWeights) learningData[type].patternWeights = { ...DEFAULT_PATTERN_WEIGHTS };
  if (!learningData[type].patternStats[patternId]) {
    learningData[type].patternStats[patternId] = { total: 0, correct: 0, accuracy: 0.5, recentResults: [] };
  }
  const s = learningData[type].patternStats[patternId];
  s.total++; if (isCorrect) s.correct++;
  s.recentResults.push(isCorrect ? 1 : 0); if (s.recentResults.length > 20) s.recentResults.shift();
  s.accuracy = s.correct / s.total;
  
  let oldW = learningData[type].patternWeights[patternId] || 1.0;
  if (s.recentResults.length >= 5) {
    const recentAcc = s.recentResults.reduce((a, b) => a + b, 0) / s.recentResults.length;
    if (recentAcc > 0.6) learningData[type].patternWeights[patternId] = Math.min(2.0, oldW * 1.05);
    else if (recentAcc < 0.4) learningData[type].patternWeights[patternId] = Math.max(0.3, oldW * 0.95);
  }
}

async function verifyPredictions(type, currentData) {
  let updated = false;
  for (const pred of learningData[type].predictions) {
    if (pred.verified) continue;
    const actualResult = currentData.find(d => d.Phien.toString() === pred.phien);
    if (actualResult) {
      pred.verified = true; pred.actual = actualResult.Ket_qua;
      pred.isCorrect = (pred.prediction === 'Tài' || pred.prediction === 'tai' ? 'Tài' : 'Xỉu') === actualResult.Ket_qua;
      
      if (pred.isCorrect) {
        learningData[type].correctPredictions++;
        learningData[type].streakAnalysis.wins++;
        learningData[type].streakAnalysis.currentStreak = Math.max(1, learningData[type].streakAnalysis.currentStreak + 1);
        learningData[type].streakAnalysis.bestStreak = Math.max(learningData[type].streakAnalysis.bestStreak, learningData[type].streakAnalysis.currentStreak);
      } else {
        learningData[type].streakAnalysis.losses++;
        learningData[type].streakAnalysis.currentStreak = Math.min(-1, learningData[type].streakAnalysis.currentStreak - 1);
        learningData[type].streakAnalysis.worstStreak = Math.min(learningData[type].streakAnalysis.worstStreak, learningData[type].streakAnalysis.currentStreak);
      }
      learningData[type].recentAccuracy.push(pred.isCorrect ? 1 : 0);
      if (learningData[type].recentAccuracy.length > 50) learningData[type].recentAccuracy.shift();
      
      if (pred.patterns) {
        pred.patterns.forEach(pName => {
          const pId = getPatternIdFromName(pName);
          updateStatsAndWeights(type, pId, pred.isCorrect);
        });
      }
      updated = true;
    }
  }
  if (updated) { learningData[type].lastUpdate = new Date().toISOString(); saveLearningData(); }
}

function normalizeResult(res) { return (res === 'Tài' || res === 'tài') ? 'tai' : 'xiu'; }

// ==================== HÀM XỬ LÝ ĐA LUỒNG CORE CHÍNH ====================
async function processPredictionForType(type, url) {
  const data = await fetchData(url, type);
  if (!data || data.length === 0) return null;
  
  await verifyPredictions(type, data);
  const nextPhien = data[0].Phien + 1;
  
  const workerResult = await runAnalysisWorker(data, type);
  
  if (lastProcessedPhien[type] !== nextPhien) {
    const record = { phien_hien_tai: nextPhien.toString(), du_doan: normalizeResult(workerResult.prediction), ti_le: `${workerResult.confidence}%`, id: '@tranhoang2286', timestamp: new Date().toISOString() };
    predictionHistory[type].unshift(record);
    if (predictionHistory[type].length > MAX_HISTORY) predictionHistory[type] = predictionHistory[type].slice(0, MAX_HISTORY);
    
    learningData[type].predictions.unshift({ phien: nextPhien.toString(), prediction: workerResult.prediction, confidence: workerResult.confidence, patterns: workerResult.factors, timestamp: new Date().toISOString(), verified: false, actual: null, isCorrect: null });
    learningData[type].totalPredictions++;
    if (learningData[type].predictions.length > 500) learningData[type].predictions = learningData[type].predictions.slice(0, 500);
    
    lastProcessedPhien[type] = nextPhien;
  }
  
  return { phien_hien_tai: nextPhien.toString(), du_doan: normalizeResult(workerResult.prediction), ti_le: `${workerResult.confidence}%`, id: '@tranhoang2286', factors: workerResult.factors };
}

async function autoProcessPredictions() {
  await processPredictionForType('hu', API_URL_HU);
  await processPredictionForType('md5', API_URL_MD5);
  savePredictionHistory();
  saveLearningData();
}

// ==================== EXPRESS ROUTES API ====================
app.get('/', (req, res) => { res.type('text/plain; charset=utf-8').send('@tranhoang2286 - Luck8 High Performance Prediction Engine'); });

app.get('/luck8-hu', async (req, res) => {
  const result = await processPredictionForType('hu', API_URL_HU);
  if (!result) return res.status(500).json({ error: 'Data fetch failed' });
  res.json({ phien_hien_tai: result.phien_hien_tai, du_doan: result.du_doan, ti_le: result.ti_le, id: result.id });
});

app.get('/luck8-md5', async (req, res) => {
  const result = await processPredictionForType('md5', API_URL_MD5);
  if (!result) return res.status(500).json({ error: 'Data fetch failed' });
  res.json({ phien_hien_tai: result.phien_hien_tai, du_doan: result.du_doan, ti_le: result.ti_le, id: result.id });
});

app.get('/luck8-hu/lichsu', (req, res) => res.json({ type: 'Luck8 - Tài Xỉu Hũ', history: predictionHistory.hu, total: predictionHistory.hu.length }));
app.get('/luck8-md5/lichsu', (req, res) => res.json({ type: 'Luck8 - Tài Xỉu MD5', history: predictionHistory.md5, total: predictionHistory.md5.length }));

app.get('/reset-learning', (req, res) => {
  learningData = {
    hu: { predictions: [], patternStats: {}, totalPredictions: 0, correctPredictions: 0, patternWeights: { ...DEFAULT_PATTERN_WEIGHTS }, lastUpdate: null, streakAnalysis: { wins: 0, losses: 0, currentStreak: 0, bestStreak: 0, worstStreak: 0 }, adaptiveThresholds: {}, recentAccuracy: [] },
    md5: { predictions: [], patternStats: {}, totalPredictions: 0, correctPredictions: 0, patternWeights: { ...DEFAULT_PATTERN_WEIGHTS }, lastUpdate: null, streakAnalysis: { wins: 0, losses: 0, currentStreak: 0, bestStreak: 0, worstStreak: 0 }, adaptiveThresholds: {}, recentAccuracy: [] }
  };
  saveLearningData();
  res.json({ message: 'Learning data reset successfully' });
});

// Khởi chạy máy chủ API chính
loadLearningData();
loadPredictionHistory();

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`[Multi-threaded Server Main] Running on port ${PORT}`);
  await fetchData(API_URL_HU, 'hu');
  await fetchData(API_URL_MD5, 'md5');
  setInterval(autoProcessPredictions, AUTO_SAVE_INTERVAL);
});
