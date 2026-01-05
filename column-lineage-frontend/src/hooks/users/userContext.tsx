import { createContext } from "react";

import { IAmplifyUser } from "@/domain/Users";

interface IUserContext {
  user: IAmplifyUser | undefined;
  setUser: (user: IAmplifyUser | undefined) => void;
}

export const UserContext = createContext<IUserContext | undefined>(undefined);