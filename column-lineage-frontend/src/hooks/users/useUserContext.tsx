import { useContext } from "react";

import { IAmplifyUser } from "@/domain/Users";
import { UserContext } from "@/hooks/users/userContext";

const useUserContext = (): IAmplifyUser => {
  const context = useContext(UserContext);
  if (!context) {
    throw new Error("useUserContext must be used within a UserProvider");
  }

  if (!context.user) {
    throw new Error("User is not authenticated");
  }

  return context.user;
};

export default useUserContext;