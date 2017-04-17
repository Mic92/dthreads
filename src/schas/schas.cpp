/*
  Author: Maksym Planeta

  Copyright (c) 2015 Maksym Planeta, TU Dresden.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

/*
 * @file   schas.h
 * @brief  Schedule assembler: Text to binary representation of schedule.
 * @author Maksym Planeta mplaneta@os.inf.tu-dresden.de
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <map>
#include <vector>

#include <stdio.h>

using namespace std;

// Thunk schedule represented as a map
map<int, map<int, int> > t_map;

int main(int argc, char *argv[])
{
  if ((argc < 2) || (argc > 2)) {
    fprintf(stderr, "Use: %s <text_schedule>\n", argv[0]);
    throw std::runtime_error("Wrong command line");
  }

  ifstream tsched;
  tsched.open(argv[1]);

  int i = 0;
  std::string line;
  while (std::getline(tsched, line)) {
    std::istringstream iss(line);
    int pid, tid, cpu; // Process id, Thunk id, CPU number

    ++i;

    if (!(iss >> pid >> tid >> cpu)) {
      // error
      fprintf(stderr, "Error in line %d\n", i);
      throw std::runtime_error("Wrong thunk schedule format");
    }

    t_map[pid][tid] = cpu;
    // cout << pid << "  " << tid << "  "  << cpu << endl;
  }

  ofstream tsched_out;
  tsched_out.open(string(argv[1]) + ".bin", ofstream::out | ofstream::binary);
  for (auto &thread : t_map) {
    int size = t_map[thread.first].size();
    tsched_out.write((char *)&thread.first, sizeof thread.first);
    tsched_out.write((char *)&size, sizeof size);
    cout << thread.first << "(" << sizeof thread.first << ")  "
         << size << "(" << sizeof size << ")  "  << endl;
    cout << "== " << endl;
    for (auto &thunk : t_map[thread.first]) {
      cout << thunk.first << "(" << sizeof thunk.first << ")  "
           << thunk.second<< "(" << sizeof thunk.second << endl;
      tsched_out.write((char *)&thunk.first, sizeof thunk.first);
      tsched_out.write((char *)&thunk.second, sizeof thunk.second);
    }
    cout << "--- " << endl;
  }
}
