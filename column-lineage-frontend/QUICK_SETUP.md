# 🚀 Quick AWS Cognito Setup

## 1. AWS Console Steps (5 minutes)

### Create User Pool:
1. **AWS Console** → **Amazon Cognito** → **Create user pool**
2. **Sign-in**: Email ✅
3. **Security**: No MFA, Email recovery ✅
4. **Sign-up**: Enable self-registration ✅, Required: email + name ✅
5. **Messages**: Send email with Cognito ✅
6. **Integration**:
   - Pool name: `column-analysis-users`
   - Hosted UI: ✅ Enable
   - Domain: `column-analysis-auth` (choose unique)
   - Client: `column-analysis-client`
   - No client secret ❌
   - Callback: `http://localhost:3000/`
   - Sign-out: `http://localhost:3000/logout`

## 2. Get Your Values

After creation, copy these from AWS Console:

```bash
# From User Pool Overview page:
User Pool ID: us-east-1_XXXXXXXXX

# From App Integration tab:
App Client ID: your-app-client-id
Domain: column-analysis-auth.auth.us-east-1.amazoncognito.com
```

## 3. Update .env File

Replace placeholders in `.env`:

```env
VITE_AWS_COGNITO_OAUTH_DOMAIN=column-analysis-auth.auth.us-east-1.amazoncognito.com
VITE_AMPLIFY_USERPOOL_ID=us-east-1_XXXXXXXXX
VITE_AMPLIFY_WEBCLIENT=your-app-client-id
```

## 4. Test Setup

```bash
npm install
npm run validate-config  # Check configuration
npm run dev              # Start app
```

## 🎯 That's it!

Your app will redirect to Cognito for authentication. Create a new account to test!