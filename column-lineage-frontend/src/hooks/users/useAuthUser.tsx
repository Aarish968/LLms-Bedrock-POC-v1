import { Auth } from "aws-amplify";
import { useEffect, useState } from "react";

import { fetchWhoAmI, clearWhoAmI } from "@/api/users";
import {
  AccessTokenPayload,
  IAmplifyUser,
  ICognitoUser,
  IUserCognitoGroups,
} from "@/domain/Users";

type TIsPropertyKey<T extends string> = T extends `is${infer Rest}`
  ? Rest extends Capitalize<Rest>
    ? T
    : never
  : never;
type TExtractIsProperties<T> = {
  [K in keyof T as TIsPropertyKey<string & K>]: T[K];
};

type TUserGroups = TExtractIsProperties<IUserCognitoGroups>;

const getUserGroupsFromToken = (
  userGroups: AccessTokenPayload,
): TUserGroups => {
  const cognitoGroups = userGroups["cognito:groups"] || [];

  return {
    isAdmin: cognitoGroups.includes("column_admin"),
    isAnalyst: cognitoGroups.includes("column_analyst"),
    isViewer: cognitoGroups.includes("column_viewer"),
  };
};

// Internal interface for tracking user during authentication
interface IInternalAmplifyUser extends Omit<IAmplifyUser, "dc_user_id"> {
  dc_user_id?: number;
}

export const getUserFromToken = async (): Promise<IAmplifyUser | undefined> => {
  try {
    const userData: ICognitoUser = await Auth.currentAuthenticatedUser();
    localStorage.setItem(
      "jwt",
      userData.signInUserSession.accessToken.jwtToken,
    );
    const userSession = userData.signInUserSession;
    const accessToken = userSession.accessToken;
    const userGroups = accessToken.payload;
    const { username } = userData;
    const email = username.replace("duo_", "");
    const parsedGroups = getUserGroupsFromToken(userGroups);

    // Create an internal user object without dc_user_id initially
    const internalUser: IInternalAmplifyUser = {
      ...userData,
      ...parsedGroups,
      email,
    };

    // Fetch the user ID from the API (or use a default for demo)
    let userId: number;
    try {
      userId = await fetchWhoAmI();
    } catch (error) {
      // For demo purposes, use a default user ID
      userId = 1;
      console.warn("Using default user ID for demo purposes");
    }

    // Return the complete user with dc_user_id as a non-null value
    return {
      ...internalUser,
      dc_user_id: userId,
    } as IAmplifyUser;
  } catch (e) {
    console.error("Authentication error:", e);
    await clearWhoAmI();
    await Auth.federatedSignIn();
    return undefined;
  }
};

type TUseAuthUser = {
  user: IAmplifyUser | undefined;
  setUser: (user: IAmplifyUser | undefined) => void;
  refreshUser: () => void;
  isLoading: boolean;
};

const useAuthUser = (): TUseAuthUser => {
  /**
   * Note: This hook should only be used in the App.tsx file. Usage elsewhere
   * will cause race conditions and unexpected behavior.
   *
   * You likely want to use the `useUserContext` hook instead.
   *
   * Hook adds the additional attributes to make an IAmplifyUser
   * If it fails at any point, it will redirect to the login page
   */

  const [user, setUser] = useState<IAmplifyUser>();
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // On mount, get the user from the token. This ensures that we start
    // with the user if they are already logged in or we redirect to the
    // login page if they are not.
    (async () => {
      await refreshUser();
    })();
  }, []);

  const refreshUser = async () => {
    setIsLoading(true);
    const user = await getUserFromToken();
    setUser(user);
    setIsLoading(false);
  };

  return { user, setUser, refreshUser, isLoading };
};

export default useAuthUser;