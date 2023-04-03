Name: ob-configserver
Summary: configserver for oceanbase database
Group: alipay/oceanbase
Version: 1.0.0
Release: %(echo $RELEASE)%{?dist}
URL: https://github.com/oceanbase/oceanbase
License: MulanPSL - 2.0
BuildArch: x86_64 aarch64

%description
configserver for oceanbase database

%define _prefix /home/admin

%build
cd $OLDPWD/../
make build-release

%install
cd $OLDPWD/../
mkdir -p $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/
mkdir -p $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/bin
mkdir -p $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/conf
mkdir -p $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/log
mkdir -p $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/run

cp bin/*                $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/bin
cp -r etc/*             $RPM_BUILD_ROOT/%{_prefix}/ob-configserver/conf

%files
%defattr(755,admin,admin)
%dir %{_prefix}/ob-configserver/
%dir %{_prefix}/ob-configserver/bin
%dir %{_prefix}/ob-configserver/conf
%dir %{_prefix}/ob-configserver/log
%dir %{_prefix}/ob-configserver/run
%{_prefix}/ob-configserver/bin/ob-configserver
%{_prefix}/ob-configserver/conf/*.yaml
