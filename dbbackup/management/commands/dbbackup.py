"""
Save backup files to Dropbox.
"""
import re
import datetime
import tempfile
import tarfile
from ... import utils
from ...dbcommands import DBCommands
from ...dbcommands import DATE_FORMAT
from ...storage.base import BaseStorage
from ...storage.base import StorageError
from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.core.management.base import LabelCommand
from optparse import make_option
import logging
logger = logging.getLogger(__name__)

DATABASE_KEYS = getattr(settings, 'DBBACKUP_DATABASES', settings.DATABASES.keys())


class Command(LabelCommand):
    help = "dbbackup [-c] [-d <dbname>] [-s <servername>]"
    option_list = BaseCommand.option_list + (
        make_option("-c", "--clean", help="Clean up old backup files", action="store_true", default=False),
        make_option("-d", "--database", help="Database to backup (default: everything)"),
        make_option("-s", "--servername", help="Specifiy server name to include in backup filename"),
    )

    @utils.email_uncaught_exception
    def handle(self, **options):
        """ Django command handler. """
        try:
            self.clean = options.get('clean')
            self.database = options.get('database')
            self.servername = options.get('servername')
            self.storage = BaseStorage.storage_factory()
            database_keys = (self.database,) if self.database else DATABASE_KEYS
            for database_key in database_keys:
                database = settings.DATABASES[database_key]
                self.dbcommands = DBCommands(database)
                self.save_new_backup(database)
                self.cleanup_old_backups(database)
        except StorageError, err:
            raise CommandError(err)

    # def save_new_backup(self, database):
    #     """ Save a new backup file. """
    #     print "Backing Up Database: %s" % database['NAME']
    #     backupfile = tempfile.SpooledTemporaryFile(max_size=10*1024*1024)
    #     backupfile.name = self.dbcommands.filename(self.servername)
    #     self.dbcommands.run_backup_commands(backupfile)
    #     print "  Backup tempfile created: %s (%s)" % (backupfile.name, utils.handle_size(backupfile))
    #     print "  Writing file to %s: %s" % (self.storage.name, self.storage.backup_dir())
    #     self.storage.write_file(backupfile)

    def save_new_backup(self, database):
        """
        Overwrite original function
        """
        max_size = 10*1024*1024

        # dump db
        print "Backing Up Database: %s" % database['NAME']
        dbdump = tempfile.SpooledTemporaryFile(max_size=max_size)
        self.dbcommands.run_backup_commands(dbdump)
        print "DB dump tempfile created: %s" % utils.handle_size(dbdump)
        dbdump.seek(0)

        # create backup file
        backupfile = tempfile.SpooledTemporaryFile(max_size=max_size)
        backupfile.name = self.dbcommands.filename(self.servername)

        # create tar file inside backup file
        compression = getattr(settings, 'BACKUP_COMPRESSION', 'bz2')
        tar = tarfile.open(mode='w:%s' % compression, fileobj=backupfile)

        # add database dump
        tarinfo = tar.gettarinfo(arcname='dump.sql', fileobj=dbdump)
        dbdump.seek(0)
        tar.addfile(tarinfo, fileobj=dbdump)
        dbdump.close()

        # add to archive directories
        backup_directories = getattr(settings, 'BACKUP_DIRECTORIES', {})
        for dirname in backup_directories:
            tar.add(backup_directories[dirname], dirname)

        tar.close()
        backupfile.seek(0)

        print "Compressed tempfile created: %s (%s)" % (
            backupfile.name, utils.handle_size(backupfile))

        print "Writing file to %s: %s" % (
            self.storage.name, self.storage.backup_dir())
        self.storage.write_file(backupfile)

    def cleanup_old_backups(self, database):
        """ Cleanup old backups.  Delete everything but the last 10
            backups, and any backup that occur on first of the month.
        """
        if self.clean:
            print "Cleaning Old Backups for: %s" % database['NAME']
            filepaths = self.storage.list_directory()
            filepaths = self.dbcommands.filter_filepaths(filepaths)
            for filepath in sorted(filepaths[0:-10]):
                regex = self.dbcommands.filename_match(self.servername, '(.*?)')
                datestr = re.findall(regex, filepath)[0]
                dateTime = datetime.datetime.strptime(datestr, DATE_FORMAT)
                if int(dateTime.strftime("%d")) != 1:
                    print "  Deleting: %s" % filepath
                    self.storage.delete_file(filepath)
