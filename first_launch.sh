echo '>>>>> Install numpy'
pip3 install numpy

echo '>>>>> Copying a config file'
cp config.conf.defaults config.conf

echo '>>>>> Command to run:'
echo 'python3 test-ctlc.py run -f config.conf'
