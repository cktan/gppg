import sys, os, csv, subprocess
from StringIO import StringIO

# -----------------------------------------------------
def _pr(prefix, s):
    for line in s.split('\n'):
        print prefix,line

pr = lambda prefix, s: _pr(prefix, s)
def pr_inf(s): pr('inf', s)
def pr_dst(s): pr('dst', s)
def pr_src(s): pr('src', s)

def set_verbose(flag):
    global pr
    if flag:
        pr = lambda prefix, s: _pr(prefix, s)
    else:
        pr = lambda prefix, s: None

set_verbose(False)

# -----------------------------------------------------
def dq(s):
    c = [c for c in s if c not in 'abcdefghijklmnopqrstuvwxyz_0123456789']
    if c: return '"%s"' % s
    if s in ['user', 'filter']:
        return '"%s"' % s
    return s

# -----------------------------------------------------
class Attr:
    def __init__(self):
        self.cname = ''
        self.datatype = ''
        self.numericprecision = 0
        self.numericscale = 0
        self.charmaxlen = 0

    def typeClause(self):
        a = self
        t = a.datatype
        if t == 'numeric':
            if a.numericprecision and a.numericscale:
                t = '%s(%s,%s)' % (t, a.numericprecision, a.numericscale)
            elif a.numericprecision:
                t = '%s(%s)' % (t, a.numericprecision)
        elif t == 'character varying':
            if a.charmaxlen:
                t = 'varchar(%s)' % a.charmaxlen
            else:
                t = 'varchar'
        elif t == 'character':
            if a.charmaxlen: t = 'character(%s)' % a.charmaxlen
        elif t == 'json':
            t = 'text'
        return t

# -----------------------------------------------------
class TableInfo:
    def __init__(self):
        self.attr = []            # array of Attr

    def isSubset(self, t):
        for i in xrange(len(self.attr)):
            a = self.attr[i]
            b = t.attr[i]
            if a.cname != b.cname: return False
            if a.typeClause() != b.typeClause(): return False
        return True
    
    def isEquiv(self, t):
        if len(self.attr) != len(t.attr):
            return False
        return self.isSubset(t);

    def columnClause(self):
        s = [dq(a.cname) for a in self.attr]
        return '\n,'.join(s)

    def columnClauseEx(self):
        s = []
        for a in self.attr:
            if a.datatype == 'json':
                t = 'substring(%s::text from 1 to 20000) as %s' % (dq(a.cname), dq(a.cname))
            if a.typeClause() == 'varchar' or a.datatype == 'text':
                t = 'substring(%s from 1 for 5000) as %s' % (dq(a.cname), dq(a.cname))
            else:
                t = dq(a.cname)
            s += [t]
        return '\n,'.join(s)

    def columnAndTypeClause(self):
        s = []
        for a in self.attr:
            s += ["%s %s" % (dq(a.cname), a.typeClause())]
        return '\n,'.join(s)


# -----------------------------------------------------
class DB:
    def __init__(self):
        self.host = ''
        self.port = ''
        self.user = ''
        self.passwd = ''
        self.dbname = ''
        self.sname = ''
        self.tname = ''
        self.prefix = ''

    def set_prefix(self, prefix):
        self.prefix = prefix
        
    def psql_raw(self, sql, stdin=None, stdout=None, stderr=None):
        env = os.environ.copy()
        if not self.passwd:
            passwd_key = ('PGPASSWORD_%s_%s' % (self.dbname, self.user)).upper()
            self.passwd = (env.get(passwd_key, '')).strip()
            if not self.passwd:
                sys.exit('env %s not set' % passwd_key)
        env['PGHOST'] = self.host
        env['PGPORT'] = self.port
        env['PGUSER'] = self.user
        env['PGPASSWORD'] = self.passwd
        env['PGDATABASE'] = self.dbname
        env['PGOPTIONS'] = '--client-min-messages=warning'
        return subprocess.Popen(['psql', '-qAt', '-c', sql], env=env, stdin=stdin, stdout=stdout, stderr=stderr)

    def psql(self, sql):
        p = self.psql_raw(sql, stdout=subprocess.PIPE)
        s = p.stdout.read()
        rc = p.wait()
        if rc:
            sys.exit('ERROR: %s' % sql)
        return s.strip()

    def psql_quiet(self, sql):
        p = self.psql_raw(sql, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            s = p.stdout.read(1024*8)
            if not s: break
        rc = p.wait()
        return rc


    def getTableInfo(self):
        '''Returns the attributes of the table. If Table is not accessible or if table does not exist, return None.'''
        t = TableInfo()
        sql = '''copy /* get columns of a table */
          (select column_name, data_type, numeric_precision, numeric_scale, character_maximum_length
              from information_schema.columns
              where table_schema='%s' and table_name='%s' and column_name != 'recxmin'
              order by ordinal_position) to stdout with csv header''' % (self.sname, self.tname)
        s = self.psql(sql)
        for row in csv.DictReader(StringIO(s)):
            if row['data_type'] == 'USER-DEFINED': continue
            if row['data_type'] == 'xid':  continue

            a = Attr()
            a.cname = row['column_name']
            a.datatype = row['data_type']
            a.numericprecision = row['numeric_precision']
            a.numericscale = row['numeric_scale']
            a.charmaxlen = row['character_maximum_length']
            t.attr += [a]

        if len(t.attr) == 0:
            return None

        return t

