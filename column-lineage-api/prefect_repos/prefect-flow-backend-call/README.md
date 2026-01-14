Setting up calling the backend with a new user: 

- Create a new user in the Amazon Cognito User Pool
- Create .env file in execution envionment with the following variables: 
```
USERNAME=<username from newly created user>
PASSWORD=<whatever you want the password to be for this new user>
temp_pass=<temporary password created after creating the new user>
USER_POOL_ID=<id of the user pool you created the new user in>
CLIENT_ID=<client id from the user pool, which can be found under the app integration tab at the very bottom>
USER_EMAIL=<email used to create the user>
```
- run initial_setup.py 
- Update environment variables with new username/password

call_backend.py contins a flow that will call the backend after the initial setup above is done