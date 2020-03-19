package org.bonitasoft.truckmilk.sonarqube;


public class SonarQubeAPI {

    /** to connect
    async static Task Main(string[] args)
        {
            var url = "https://sonarcloud.io/api/project_branches/list?project=DevProject";
            string token = "TODO - your token goes here";

            var client = new HttpClient
            {
                DefaultRequestHeaders =
                {
                    Authorization = GetBasicAuthTokenHeader(token)
                }
            };

            var response = await client.GetAsync(url);
            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine("Call succeeded:");
                var content = await response.Content.ReadAsStringAsync();
                Console.WriteLine(content);
            }
            else
            {
                Console.WriteLine("Call failed:");
                Console.WriteLine(response.ToString());
            }
        }

        private static AuthenticationHeaderValue GetBasicAuthTokenHeader(string token)
        {
            // The basic token needs to be base-64 encoded.
            // Also, it's expected to be in the form "username:password". If you are using a 
            // token you supply it in place of the username and leave the password blank i.e. "token:"
            var encodedToken = Convert.ToBase64String(Encoding.UTF8.GetBytes(token + ":"));
            var authHeader = new AuthenticationHeaderValue("Basic", encodedToken);

            return authHeader;
        }
        */
}
