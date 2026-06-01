# test-ctlc — VAS Experts OIMC/СОРМ binding-quality auditor
#
# Builds for CentOS Stream 8 (system python3.6). Python dependencies
# (psycopg2, numpy) are shipped as a bundled virtualenv so the package is
# self-contained and does not depend on EPEL / pip on the target host.

%global instroot   /opt/vasexperts
%global appdir     %{instroot}/var/lib/test-ctlc
%global venvdir    %{appdir}/venv
%global etcdir     %{instroot}/etc/test-ctlc
%global logdir     %{instroot}/var/log/test-ctlc
%global tmpdir     %{appdir}/tmp

# We ship a prebuilt virtualenv. Don't let rpm rewrite its shebangs, recompile
# its bytecode, or auto-generate requires/provides from the bundled .so files.
%global __brp_mangle_shebangs %{nil}
%global __brp_python_bytecompile %{nil}
%global debug_package %{nil}
AutoReqProv: no

Name:           test-ctlc
Version:        2.0.0
Release:        1%{?dist}
Summary:        VAS Experts OIMC/СОРМ traffic binding-quality auditor

License:        Proprietary
URL:            https://github.com/Etaminchik/test-ctlc
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  python3 >= 3.6
BuildRequires:  python3-devel
BuildRequires:  gcc
Requires:       python3 >= 3.6

%description
Python tooling that audits the data quality ("binding percentage" / привязка)
of captured NAT, AAA and login records in a VAS Experts OIMC/СОРМ
lawful-interception PostgreSQL database, and dumps offending records for
investigation.

Installs under %{instroot}:
  bin/test-ctlc                  - CLI entry point
  etc/test-ctlc/config.conf      - configuration (preserved on upgrade)
  var/lib/test-ctlc/{lib,venv}   - code and bundled python environment
  var/log/test-ctlc              - logs

%prep
%setup -q

%build
# Build the bundled virtualenv at its final runtime path so that every
# embedded path (shebangs, byte-compiled modules) is already correct and the
# rpmbuild buildroot string never leaks into shipped files.
rm -rf %{venvdir}
%{__python3} -m venv %{venvdir}
%{venvdir}/bin/pip install --upgrade pip wheel
%{venvdir}/bin/pip install psycopg2-binary numpy

%install
rm -rf %{buildroot}

# --- bundled virtualenv ---
mkdir -p %{buildroot}%{appdir}
cp -a %{venvdir} %{buildroot}%{venvdir}

# --- python package (lib/) ---
cp -a lib %{buildroot}%{appdir}/lib

# --- standalone helper tools ---
cp -a tools %{buildroot}%{appdir}/tools

# --- CLI entry point ---
mkdir -p %{buildroot}%{instroot}/bin
install -m 0755 test-ctlc.py %{buildroot}%{instroot}/bin/test-ctlc
# Point the entry point at the bundled venv interpreter.
sed -i '1s|^#!.*$|#!%{venvdir}/bin/python3|' %{buildroot}%{instroot}/bin/test-ctlc

# --- default configuration ---
mkdir -p %{buildroot}%{etcdir}
install -m 0644 config.conf.defaults %{buildroot}%{etcdir}/config.conf

# --- runtime directories ---
mkdir -p %{buildroot}%{logdir}
mkdir -p %{buildroot}%{tmpdir}

%files
%{instroot}/bin/test-ctlc
%{appdir}/lib
%{appdir}/venv
%{appdir}/tools
%config(noreplace) %{etcdir}/config.conf
%dir %{logdir}
%dir %{tmpdir}

%clean
rm -rf %{buildroot}
rm -rf %{venvdir}

%changelog
* Mon Jun 01 2026 VAS Experts <noreply@vasexperts.local> - 2.0.0-1
- Initial RPM packaging with FHS layout under /opt/vasexperts.
- Bundled virtualenv (psycopg2, numpy); CLI installed as /opt/vasexperts/bin/test-ctlc.
- Removed billing/ tooling; standalone helpers moved under tools/.
