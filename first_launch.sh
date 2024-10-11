echo '>>>>> Install python3 and pip3'
yum install python3 -y
yum install python3-pip -y

echo '>>>>> Install numpy'
pip3 install numpy

echo '>>>>> Copying a config file'
cp config.conf.defaults config.conf

echo '>>>>> Command to run:'
echo 'python3 test-ctlc.py run -f config.conf'
