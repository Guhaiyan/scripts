#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <map>

int main(int argc, char **argv)
{
	std::string pwd = "/home/centos/rocksdb_100M_1/hbase";
	std::string source_file = pwd + "/iostat_thread10_fs";
	std::ifstream iostat(source_file.data());
	std::string read_file = pwd + "/read_perf.txt";
	std::ofstream read_perf(read_file.data());
	std::string write_file = pwd + "/write_perf.txt";
	std::ofstream write_perf(write_file.data());
	std::string util_file = pwd + "/util.txt";
	std::ofstream util_perf(util_file.data());
	std::string cnt, word, value, write, read;
	int write_index;

	while(getline(iostat, cnt)) {
		std::istringstream line(cnt);
		write_index = 0;
		while (line >> word) {
			++write_index;
			if (word == "%util") {
				getline(iostat, cnt);
				std::istringstream v_line(cnt);
				while (write_index) {
					v_line >> value;
					if (write_index == 8)
						write = value;
					if (write_index == 9)
						read = value;
					--write_index;
				}
				util_perf << value << std::endl;
				write_perf << write << std::endl;
				read_perf << read << std::endl;	
			}
		}
	}

}
