# ceres
database for issue tracking

You will need to create a new folder in your home directory `.ssh`
in that, you will need the file `config` with the content added

```
Host github.com
  StrictHostKeyChecking no
  IdentityFile ~/.ssh/{name_of_your_github_private_ssh_key}
```

Then copy your public ssh key and send it to me


To configure the database:
Create a folder in your home director named `.ceres`
in it, put the file `auth.nfo`
In the auth file, add the following lines

```commandline
SERVER~ceres
DB~ceresdb
USERNAME~dwarfmoon
PASSWORD~astroidbelt
```

This may seen insecure - but it's a docker container not connected to the internet

Now, using notepad, in Administrator mode, open the file C:\Windows\System32\drivers\etc\hosts
and add the follling entry at the bottom

`{IP_ADDRESS}    ceres`

I'll give you the IP address later, we don't share it publicly or put it in any files that will be pushed to github

Save it - and that's it. 

Now open MySQL Workbench and create a new Connection:
Server: ceres
Username: dwarfmoon
Password (Store in Vautl): dwarfmoon
Default Schema: ceresdb

Test, and save ... make sure I have added your home IP to the Security Group in AWS. 
Now write the software that is required. 


good connection