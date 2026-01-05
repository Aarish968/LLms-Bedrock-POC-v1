# AWS Cognito Setup Guide for Lineage Analysis

## 📋 Prerequisites
- AWS Account with appropriate permissions
- Access to AWS Console

## 🚀 Step-by-Step Setup

### 1. Create Cognito User Pool

1. **Go to AWS Console**
   - Navigate to Amazon Cognito service
   - Click "Create user pool"

2. **Configure Sign-in Experience**
   - Authentication providers: **Cognito user pool**
   - Sign-in options: **Email** ✅
   - Click "Next"

3. **Configure Security Requirements**
   - Password policy: **Cognito defaults**
   - MFA: **No MFA** (for development)
   - Account recovery: **Email only** ✅
   - Click "Next"

4. **Configure Sign-up Experience**
   - Self-service sign-up: **Enable** ✅
   - Attribute verification: **Send email verification** ✅
   - Required attributes: **email** ✅ and **name** ✅
   - Click "Next"

5. **Configure Message Delivery**
   - Email provider: **Send email with Cognito**
   - Click "Next"

6. **Integrate Your App**
   - User pool name: `column-analysis-users`
   - Hosted authentication pages: **Use the Cognito Hosted UI** ✅
   - Domain type: **Use a Cognito domain**
   - Domain prefix: `column-analysis-auth` (or choose unique name)
   - App client name: `column-analysis-client`
   - Client secret: **Don't generate** ❌
   - Allowed callback URLs: `http://localhost:3000/`
   - Allowed sign-out URLs: `http://localhost:3000/logout`
   - Click "Next"

7. **Review and Create**
   - Review all settings
   - Click "Create user pool"

### 2. Get Configuration Values

After creation, collect these values:

#### From User Pool Overview:
- **User Pool ID**: `us-east-1_XXXXXXXXX`

#### From App Integration Tab:
- **App Client ID**: `your-app-client-id`
- **Cognito Domain**: `column-analysis-auth.auth.us-east-1.amazoncognito.com`

### 3. Update Environment Variables

Update your `.env` file with the actual values:

```env
# Replace these with your actual values
VITE_AWS_REGION=us-east-1
VITE_AWS_COGNITO_OAUTH_DOMAIN=column-analysis-auth.auth.us-east-1.amazoncognito.com
VITE_SSO_REDIRECT_URI=http://localhost:3000
VITE_AMPLIFY_USERPOOL_ID=us-east-1_XXXXXXXXX
VITE_AMPLIFY_WEBCLIENT=your-app-client-id
```

### 4. Create Test Users (Optional)

1. Go to your User Pool
2. Click "Users" tab
3. Click "Create user"
4. Fill in:
   - Username: `testuser@example.com`
   - Email: `testuser@example.com`
   - Temporary password: `TempPass123!`
   - Uncheck "Mark phone number as verified"
   - Check "Mark email as verified"
5. Click "Create user"

### 5. Test the Setup

1. Start your React app: `npm run dev`
2. Navigate to `http://localhost:3000`
3. You should be redirected to Cognito Hosted UI
4. Sign up with a new account or use the test user

## 🔧 Troubleshooting

### Common Issues:

1. **"Invalid redirect URI"**
   - Ensure callback URL in Cognito matches exactly: `http://localhost:3000/`

2. **"Domain not found"**
   - Check the domain prefix is unique
   - Wait a few minutes for domain to propagate

3. **"Client does not exist"**
   - Verify App Client ID is correct
   - Ensure client secret is NOT generated

### Verification Steps:

1. **Test Cognito Domain**: Visit your domain URL directly
   - Should show Cognito sign-in page
   
2. **Check Environment Variables**: 
   ```bash
   echo $VITE_AMPLIFY_USERPOOL_ID
   echo $VITE_AMPLIFY_WEBCLIENT
   ```

3. **Browser Console**: Check for any authentication errors

## 📝 Production Setup

For production deployment, you'll need to:

1. **Update Callback URLs**:
   - Add your production domain: `https://yourdomain.com/`
   - Add logout URL: `https://yourdomain.com/logout`

2. **Custom Domain** (Optional):
   - Set up custom domain for Cognito
   - Update `VITE_AWS_COGNITO_OAUTH_DOMAIN`

3. **Security Enhancements**:
   - Enable MFA
   - Configure advanced security features
   - Set up proper IAM roles

## 🎯 Next Steps

After setup:
1. Test user registration and login
2. Verify JWT tokens are working
3. Test logout functionality
4. Configure user groups/roles if needed

## 📞 Support

If you encounter issues:
1. Check AWS CloudWatch logs
2. Verify all environment variables
3. Test with a fresh browser session
4. Check AWS Cognito service status