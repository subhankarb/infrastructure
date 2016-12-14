import logging
import luigi
import luigi.s3
import luigi.contrib.ssh
import luigi.date_interval
import requests
import os

class Mirai360Day(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget("data/mirai360-{}".format(self.date))

    def run(self):
        """
        Get the file from the API then return it locally
        :return:
        """
        with self.output().open('w') as outfile:
            r = requests.get('http://localhost:8000/api/{}'.format(self.date))
            if r.status_code == 200:
                outfile.write(r.text)
            else:
                raise RuntimeError


class Mirai360Week(luigi.Task):
    week = luigi.DateIntervalParameter()

    def requires(self):
        return [ Mirai360Day(d) for d in self.week.dates()]

    def output(self):
        return luigi.LocalTarget("data/mirai360-{}".format(self.week))

    def run(self):
        with self.output().open('w') as outfile:
            for i in self.input():
                print(i.open('r').read(), file=outfile)


class OpenNTPWeek(luigi.Task):
    week = luigi.DateIntervalParameter()

    def output(self):
        #return luigi.LocalTarget("data/OpenNTP/week-{}".format(self.week))
        return luigi.s3.S3Target("s3://test-bucket/raw/ntp-scan/parsed-{}".format(self.week))

    def run(self):
        ssh = luigi.contrib.ssh.RemoteFileSystem("127.0.0.1")
        dir_list = ssh.listdir("/tmp/ntp-scan/")
        found_a_file=False
        for day in self.week.dates():
            d_file = 'parsed-{}.out'.format(day)
            for f in dir_list:
                if d_file == os.path.basename(f):
                    if found_a_file:
                        raise RuntimeError("Multiple files for week")
                    logging.info("found {} in {}".format(day, dir_list))
                    tmpfile = luigi.LocalTarget(is_tmp=True)
                    ssh.get(f, tmpfile.path)
                    with self.output().open("w") as out_file, tmpfile.open('r') as in_file:
                        for line in in_file:
                            out_file.write(line)
                    found_a_file = True
            if not found_a_file:
                raise RuntimeError("didn't find anything for the week")

class ThisWeeksFiles(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        week = luigi.date_interval.Week.from_date(self.date)
        yield OpenNTPWeek(week)
        #yield Mirai360Week(week)
