import { z } from "zod";

export const UserSchema = z.object({
  user_id: z.number(),
  display_name: z.string().optional().default(""),
  email: z.string(),
});

export type IUser = z.infer<typeof UserSchema>;

export interface ICognitoUser {
  username: string; // duo_username@cisco.com
  pool: Pool;
  Session: null;
  client: Client;
  signInUserSession: SignInUserSession;
  authenticationFlowType: string;
  storage: Storage;
  keyPrefix: string;
  userDataKey: string;
  attributes: Attributes;
  preferredMFA: string;
}

export interface IUserCognitoGroups {
  isAdmin: boolean;
  isAnalyst: boolean;
  isViewer: boolean;
}

export interface IAmplifyUser extends ICognitoUser, IUserCognitoGroups {
  email: string; // username@cisco.com
  dc_user_id: number;
}

export interface Attributes {
  sub: string;
  identities: string;
  email_verified: boolean;
  name: string;
  email: string;
}

export interface Client {
  endpoint: string;
}

export interface Pool {
  userPoolId: string;
  clientId: string;
  client: Client;
  advancedSecurityDataCollectionFlag: boolean;
  storage: Storage;
}

export interface Storage {
  jwt: string;
}

export interface SignInUserSession {
  idToken: IdToken;
  refreshToken: RefreshToken;
  accessToken: AccessToken;
  clockDrift: number;
}

export interface AccessToken {
  jwtToken: string;
  payload: AccessTokenPayload;
}

export interface AccessTokenPayload {
  sub: string;
  "cognito:groups": string[];
  iss: string;
  version: number;
  client_id: string;
  origin_jti: string;
  token_use: string;
  scope: string;
  auth_time: number;
  exp: number;
  iat: number;
  jti: string;
  username: string;
}

export interface IdToken {
  jwtToken: string;
  payload: IdTokenPayload;
}

export interface IdTokenPayload {
  at_hash: string;
  sub: string;
  "cognito:groups": string[];
  email_verified: boolean;
  iss: string;
  "cognito:username": string;
  origin_jti: string;
  aud: string;
  identities: Identity[];
  token_use: string;
  auth_time: number;
  name: string;
  exp: number;
  iat: number;
  jti: string;
  email: string;
}

export interface Identity {
  userId: string;
  providerName: string;
  providerType: string;
  issuer: string;
  primary: string;
  dateCreated: string;
}