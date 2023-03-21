```
  _____                                     
_/ ____\______   ____   ____ ___________.__.
\   __\\_  __ \_/ __ \ /    \\___   <   |  |
 |  |   |  | \/\  ___/|   |  \/    / \___  |
 |__|   |__|    \___  >___|  /_____ \/ ____|
                    \/     \/      \/\/     
```

A Postgres wire protocol aware mirroring proxy.

# Getting started
Frenzy is an experimental Postgres mirroring proxy that allows mirroring production traffic to shadow instances. Frenzy allows you to service production traffic from a `primary` while measuring one or more `mirror` shadow instances.

Frenzy takes 1 `primary` connection string that will be used to respond to production requests and multiple `mirror` connection strings that will receive the duplicated traffic from the primary.

# Building
To compile the `bin/frenzy` binary run the following.
```
make
```

# Using
Provide 1 primary Postgres connection string and one or many mirror Postgres connection strings.
```
./bin/frenzy --listen :5432 --primary postgresql://postgres:password@localhost:5441/postgres --mirror postgresql://postgres:password@localhost:5442/postgres
```

# Supported Queries
Right now I am testing with a simple hello world query.
```
PGPASSWORD=password psql -U postgres -h localhost -p 5432 -c "SELECT version();"
```
Surprisingly I've seen more complicated queries work already! `\l` in the `psql` console also dispatches a more complicated query that seems to work!
