#######################
##### MySQL setup #####
#######################


# Install MySQL server
import os

# Install MySQL server
os.system("sudo apt-get update -y")
os.system("sudo apt-get install -y mysql-server")
os.system("sudo systemctl start mysql")

# Install Python libraries
os.system("pip install mysql-connector-python pandas sqlalchemy pymysql")


# Remove --skip-grant-tables from MySQL config if present
os.system("sudo sed -i 's/^skip-grant-tables//' /etc/mysql/mysql.conf.d/mysqld.cnf")
os.system("sudo sed -i 's/^skip_grant_tables//' /etc/mysql/mysql.conf.d/mysqld.cnf")

# Stop any running instance, then start clean
os.system("sudo service mysql stop")
os.system("sudo service mysql start")

# Give MySQL a moment to finish starting
import time
import mysql.connector
time.sleep(3)

# Create DB, user, grant privileges
sql = """
CREATE DATABASE IF NOT EXISTS crypto;
CREATE USER IF NOT EXISTS 'admin_user'@'localhost' IDENTIFIED BY 'StrongPassword123!';
GRANT ALL PRIVILEGES ON crypto.* TO 'admin_user'@'localhost';
FLUSH PRIVILEGES;
"""

with open("/tmp/setup.sql", "w") as f:
    f.write(sql)

os.system("sudo mysql < /tmp/setup.sql")
os.system("rm /tmp/setup.sql")
print("Database and user created.")
