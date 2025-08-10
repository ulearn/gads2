// logger.js - Centralized logging with automatic rotation
const fs = require('fs');
const path = require('path');

// Log rotation settings
const MAX_LOG_LINES = 5000;
const LOG_FILE_PATH = path.join(__dirname, 'gads.log');
const HISTORY_LOG_PATH = path.join(__dirname, 'gads_history.log');

// Store original console methods
let originalConsoleLog;
let originalConsoleError;

/**
 * Count lines in log file
 */
function countLogLines() {
  try {
    if (!fs.existsSync(LOG_FILE_PATH)) return 0;
    const data = fs.readFileSync(LOG_FILE_PATH, 'utf8');
    return data.split('\n').length;
  } catch (err) {
    return 0;
  }
}

/**
 * Rotate log file when it gets too big
 */
function rotateLogFile() {
  try {
    const lineCount = countLogLines();
    if (lineCount > MAX_LOG_LINES) {
      // Step 1: Copy current log to history file
      const currentLogData = fs.readFileSync(LOG_FILE_PATH, 'utf8');
      fs.writeFileSync(HISTORY_LOG_PATH, currentLogData);
      
      // Step 2: Clear the current log file
      const timestamp = new Date().toISOString();
      const rotationMessage = `[${timestamp}] LOG ROTATED: Previous ${lineCount} lines moved to gads_history.log\n`;
      fs.writeFileSync(LOG_FILE_PATH, rotationMessage);
      
      // Log to console (using original method to avoid recursion)
      if (originalConsoleLog) {
        originalConsoleLog(`Log file rotated: ${lineCount} lines moved to gads_history.log, current log cleared`);
      }
    }
  } catch (err) {
    if (originalConsoleError) {
      originalConsoleError('Failed to rotate log file:', err);
    }
  }
}

/**
 * Enhanced console.log with file logging and rotation
 */
function enhancedConsoleLog(...args) {
  const message = args.map(arg =>
    typeof arg === 'object' ? JSON.stringify(arg, null, 2) : arg
  ).join(' ');

  try {
    const timestamp = new Date().toISOString();
    fs.appendFileSync(LOG_FILE_PATH, `[${timestamp}] ${message}\n`);
    
    // Check for log rotation every ~100 log entries (for performance)
    if (Math.random() < 0.01) { // 1% chance to check
      rotateLogFile();
    }
  } catch (err) {
    if (originalConsoleLog) {
      originalConsoleLog('Failed to write to gads.log:', err);
    }
  }

  // Call original console.log
  if (originalConsoleLog) {
    originalConsoleLog.apply(console, args);
  }
}

/**
 * Enhanced console.error with file logging
 */
function enhancedConsoleError(...args) {
  const message = args.map(arg =>
    typeof arg === 'object' ? JSON.stringify(arg, null, 2) : arg
  ).join(' ');

  try {
    const timestamp = new Date().toISOString();
    fs.appendFileSync(LOG_FILE_PATH, `[${timestamp}] ERROR: ${message}\n`);
  } catch (err) {
    if (originalConsoleError) {
      originalConsoleError('Failed to write to gads.log:', err);
    }
  }

  // Call original console.error
  if (originalConsoleError) {
    originalConsoleError.apply(console, args);
  }
}

/**
 * Set up logging - override console methods
 */
function setupLogger() {
  // Store original methods
  originalConsoleLog = console.log;
  originalConsoleError = console.error;
  
  // Override console methods
  console.log = enhancedConsoleLog;
  console.error = enhancedConsoleError;
  
  // Log that logging is active
  console.log('Logger initialized - logging to gads.log with auto-rotation at', MAX_LOG_LINES, 'lines');
}

/**
 * Restore original console methods (for testing or cleanup)
 */
function restoreLogger() {
  if (originalConsoleLog) console.log = originalConsoleLog;
  if (originalConsoleError) console.error = originalConsoleError;
}

/**
 * Get current log file stats
 */
function getLogStats() {
  return {
    currentLines: countLogLines(),
    maxLines: MAX_LOG_LINES,
    logFile: LOG_FILE_PATH,
    historyFile: HISTORY_LOG_PATH,
    logFileExists: fs.existsSync(LOG_FILE_PATH),
    historyFileExists: fs.existsSync(HISTORY_LOG_PATH)
  };
}

/**
 * Manually trigger log rotation (for testing)
 */
function forceRotation() {
  rotateLogFile();
}

module.exports = {
  setupLogger,
  restoreLogger,
  getLogStats,
  forceRotation,
  rotateLogFile,
  MAX_LOG_LINES
};