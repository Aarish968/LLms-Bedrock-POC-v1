// Polyfills for AWS Amplify and other Node.js libraries in browser environment

// Global polyfill
if (typeof global === 'undefined') {
  (window as any).global = globalThis;
}

// Process polyfill
if (typeof process === 'undefined') {
  (window as any).process = {
    env: {},
    nextTick: (fn: Function) => setTimeout(fn, 0),
    version: '',
    platform: 'browser',
  };
}

// Buffer polyfill (if needed)
if (typeof Buffer === 'undefined') {
  try {
    const { Buffer } = require('buffer');
    (window as any).Buffer = Buffer;
  } catch (e) {
    // Buffer not available, continue without it
  }
}

export {};