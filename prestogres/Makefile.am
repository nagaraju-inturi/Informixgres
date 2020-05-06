
bin/ctl: bin/ctl.in
	sed -e 's|@@DATADIR@@|$(datarootdir)/prestogres|' -e 's|@@SYSCONFDIR@@|$(sysconfdir)|' < bin/ctl.in > bin/ctl

bin_SCRIPTS = bin/ctl

sysconf_DATA = config/informix.conf \
			   config/informix_hba.conf \
			   config/informix_passwd

pkgdata_DATA = pgsql/informix_client.py \
			   pgsql/informix.py \
			   pgsql/setup.sql \
			   config/postgresql.conf

EXTRA_DIST = bin/ctl.in \
			 $(pkgdata_DATA) \
			 $(sysconf_DATA) \
			 LICENSE \
			 NOTICE \
			 ../README.md

