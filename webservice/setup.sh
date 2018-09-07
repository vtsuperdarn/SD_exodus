sudo apt install apache2
sudo apt install apache2-doc
sudo service apache2 start

sudo apt install -y build-essential
sudo install -y libapache2-mod-proxy-html libxml2-dev
a2enmod
sudo a2enmod proxy
sudo a2enmod proxy_http
sudo a2enmod proxy_ajp
sudo a2enmod rewrite
sudo a2enmod deflate
sudo a2enmod headers
sudo a2enmod proxy_balancer
sudo a2enmod proxy_connect
sudo a2enmod proxy_html

sudo service apache2 restart
