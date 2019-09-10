# Fix Ldap to Basic Authentication issues for eXo platform

**Tested with eXo Platform 5.2**.

This tool get the first name, last name and email on the users from the social schema and create the entries on the jbid one

To run it :

```shell
docker run -ti -v $PWD:/src -w /src golang:1.13 go run fix.go
```

A default database connection url is specified to ``host.docker.internal:3306`` with the naive login password exo/exo. 
You will have to change this to connect to a real eXo database
