# mongo
hyperf mongo client

db.auth("adminUser", "adminPass")

db.createUser(
  {
    user: "adminUser",
    pwd: "adminPass",
    roles: [ { role: "dbAdmin", db: "homestead" } ]
  }
)
