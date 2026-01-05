declare global {
  namespace NodeJS {
    interface ProcessEnv {
      VITE_AWS_REGION: string;
      VITE_AWS_COGNITO_OAUTH_DOMAIN: string;
      VITE_SSO_REDIRECT_URI: string;
      VITE_AMPLIFY_USERPOOL_ID: string;
      VITE_AMPLIFY_WEBCLIENT: string;
      VITE_API_BASE_URL: string;
      VITE_APP_TITLE: string;
      VITE_ENV: string;
      NODE_ENV: "development" | "production";
      VITE_MOCKING?: "true" | "false" | "1" | "0";
    }
  }
}

export {}