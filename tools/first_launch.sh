echo '>>>>> Install python3 and pip3'
yum install python3 -y --disablerepo='pgd*'
yum install python3-pip -y --disablerepo='pgd*'

echo '>>>>> Install numpy'
pip3 install numpy

echo '>>>>> Copying a config file'
cp config.conf.defaults config.conf

echo '>>>>> Note: the recommended way to deploy is the RPM package, which installs'
echo '>>>>> the binary to /opt/vasexperts/bin/test-ctlc and the config to'
echo '>>>>> /opt/vasexperts/etc/test-ctlc/config.conf'

echo '>>>>> Command to run (from a source checkout):'
echo 'python3 test-ctlc.py run -f config.conf'
echo '>>>>> Command to run (installed):'
echo '/opt/vasexperts/bin/test-ctlc run -f /opt/vasexperts/etc/test-ctlc/config.conf'
