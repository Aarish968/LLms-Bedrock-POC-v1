#!/usr/bin/env node

/**
 * Configuration Validation Script for Lineage Analysis
 * Validates AWS Cognito environment variables
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Get current directory for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables
dotenv.config();

const requiredEnvVars = [
  'VITE_AWS_REGION',
  'VITE_AWS_COGNITO_OAUTH_DOMAIN',
  'VITE_SSO_REDIRECT_URI',
  'VITE_AMPLIFY_USERPOOL_ID',
  'VITE_AMPLIFY_WEBCLIENT'
];

console.log('🔍 Validating AWS Cognito Configuration...\n');

let hasErrors = false;

// Check if .env file exists
const envPath = path.join(__dirname, '..', '.env');
if (!fs.existsSync(envPath)) {
  console.error('❌ .env file not found!');
  console.log('📝 Please copy .env.example to .env and update the values');
  process.exit(1);
}

// Validate required environment variables
requiredEnvVars.forEach(varName => {
  const value = process.env[varName];
  
  if (!value) {
    console.error(`❌ Missing: ${varName}`);
    hasErrors = true;
  } else if (value.includes('XXXXXXXXX') || value.includes('your-')) {
    console.error(`❌ Placeholder value: ${varName} = ${value}`);
    hasErrors = true;
  } else {
    console.log(`✅ ${varName} = ${value}`);
  }
});

// Validate specific formats
if (process.env.VITE_AMPLIFY_USERPOOL_ID) {
  const userPoolId = process.env.VITE_AMPLIFY_USERPOOL_ID;
  if (!userPoolId.match(/^us-east-1_[A-Za-z0-9]+$/)) {
    console.error(`❌ Invalid User Pool ID format: ${userPoolId}`);
    console.log('   Expected format: us-east-1_XXXXXXXXX');
    hasErrors = true;
  }
}

if (process.env.VITE_AWS_COGNITO_OAUTH_DOMAIN) {
  const domain = process.env.VITE_AWS_COGNITO_OAUTH_DOMAIN;
  if (!domain.includes('.auth.us-east-1.amazoncognito.com')) {
    console.error(`❌ Invalid Cognito domain format: ${domain}`);
    console.log('   Expected format: your-domain.auth.us-east-1.amazoncognito.com');
    hasErrors = true;
  }
}

console.log('\n' + '='.repeat(50));

if (hasErrors) {
  console.error('❌ Configuration validation failed!');
  console.log('\n📋 Next steps:');
  console.log('1. Follow AWS_COGNITO_SETUP.md to create Cognito User Pool');
  console.log('2. Update .env file with actual values from AWS Console');
  console.log('3. Run this script again to validate');
  process.exit(1);
} else {
  console.log('✅ Configuration validation passed!');
  console.log('\n🚀 You can now run: npm run dev');
}

console.log('='.repeat(50));