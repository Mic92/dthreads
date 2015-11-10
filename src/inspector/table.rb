require 'json'
require 'erb'
require 'etc'
require 'pry'

if ARGV.size < 1
  puts "USAGE: #{$PROGRAM_NAME} log.json"
  exit(1)
end

USER_HZ = Etc.sysconf(Etc::SC_CLK_TCK)

module Enumerable
  def sum
    return self.inject(0){|accum, i| accum + i }
  end

  def mean
    return self.sum / self.length.to_f
  end

  def sample_variance
    m = self.mean
    sum = self.inject(0){|accum, i| accum + (i - m) ** 2 }
    return sum / (self.length - 1).to_f
  end

  def standard_deviation
    return Math.sqrt(self.sample_variance)
  end
end

def average(ary_)
  ary = ary_.dup
  ary.delete_at(ary.find_index(ary.max))
  ary.delete_at(ary.find_index(ary.min))
  ary.map!(&:to_f)
  ary.mean
end

PERF_EVENTS = [
    'branch-instructions',
    'bus-cycles',
    'cache-misses',
    'cache-references',
    'cpu-cycles',
    'instructions',
    'ref-cycles',
    'alignment-faults',
    'context-switches',
    'cpu-clock',
    'cpu-migrations',
    'major-faults',
    'minor-faults',
    'page-faults',
    'task-clock'
]

class Measurement
  attr_accessor :times,
                :log_sizes,
                :compressed_logsizes,
                :name,
                :system_time,
                :user_time,
                :time_per_cpu,
                :perf_stats
end

class Benchmark
  attr_accessor :libs, :threads, :name, :args
end

def generate(log_path)
  json = JSON.load(open(log_path))
  benchmarks = []
  json.each do |bench, data|
    b = Benchmark.new
    #bench =~ /^([^-]+)/
    #b.name = $1
    b.name = bench
    b.libs = []
    threads = data["threads"] or
      abort "no threads found in #{bench}"
    args = data["args"] or
      abort "no args found in #{bench}"
    b.args = args.map do |arg|
      if arg.is_a?(String)
        File.basename(arg)
      else
        arg
      end
    end
    b.threads = threads

    data['libs'].each do |lib, measures|
      props = %w(compressed_logsizes log_sizes system_time user_time times)
      m = Measurement.new
      m.name = lib
      props.each do |prop|
        value = measures[prop] or abort "no #{prop} found in #{bench}"
        m.send("#{prop}=", average(value))
      end
      m.perf_stats = {}
      PERF_EVENTS.each do |field|
          m.perf_stats[field] = measures[field]
      end
      m.time_per_cpu = measures['time_per_cpu']
      b.libs << m
    end
    benchmarks << b
  end
  benchmarks
end

def safe_write(path, content)
  dir = File.dirname(path)
  FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
  temp_path = path.to_s + '.tmp'
  File.open(temp_path, 'w+') do |f|
    f.write(content)
  end

  FileUtils.mv(temp_path, path)
end

class TemplateContext < OpenStruct
  def get_binding
    binding
  end

  def tex_escape(s)
    s.to_s.gsub(/([&%$#_{}~^\\])/, '\\\\\\1')
  end
end

class Template
  def initialize(template)
    @erb = ERB.new(template, nil, "-")
  end

  def render(params={})
    context = TemplateContext.new(params)
    @erb.result(context.get_binding)
  rescue => e
    raise StandardError.new("fail to render template: #{e}")
  end

  def write(path, options={})
    safe_write(path, render(options))
  end
end

#ERB_TEMPLATE = <<-EOF_
#\\documentclass{article}
#\\begin{document}
#\\begin{tabular}{<%= columns.join(" ") %>}
#  <%- rows.each do |columns| -%>
#    <%= (columns.map {|c| tex_escape(c) }).join("& ") %> \\\\
#  <%- end -%>
#\\end{tabular}
#\\end{document}
#EOF
ERB_TEMPLATE = <<-EOF
<html>
<head>

<link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet" integrity="sha256-MfvZlkHCEqatNoGiOXveE8FIwMzZg4W85qfrfIFBfYc= sha512-dTfge/zgoMYpP7QbHy4gWMEGsbsdZeCXz7irItjcC3sPUFtf0kuFbDz/ixG7ArTxmDjLXDmezHubeNikyKGVyQ==" crossorigin="anonymous">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js" integrity="sha256-Sk3nkD6mLTMOF0EOpNtsIry+s1CsaqQC1rVLTAy+0yc= sha512-K1qjQ+NcF2TYO/eI3M6v8EiNYZfA95pQumfvcVrTHtwQVDG+aHRqLi/ETn2uB+1JqwYqVG3LIvdm9lj6imS/pQ==" crossorigin="anonymous"></script>

</head>
<body>
<table class="table table-striped table-bordered">
<%- rows.each_with_index do |columns, i| -%>
<%- if i == 0 %>
<tr>
    <%- columns.each do |column| %>
    <th>
      <%= column %>
    </th>
    <%- end %>
</tr>
<%- else %>
<tr>
    <%- columns.each do |column| %>
      <td>
        <%- if column.is_a? Array %>
            <%= column.join("<br>") %>
        <%- else %>
            <%= column %>
        <%- end %>
      </td>
    <%- end %>
</tr>
<%- end %>
<%- end -%>
</table>
</body>
</html>
EOF


def to_mb(val)
  val / 1000.0 / 1000
end

def short(v)
  format('%.3f',v)
end

DEFAULT_HEADER = %w(Name Options)

BENCH_FIELDS = {
  'Library' => proc { |b| b.name },
  'Logsize [MB]' => proc { |b| short(to_mb(b.log_sizes)) },
  'Compressed Logsize [MB]' => proc { |b| short(to_mb(b.compressed_logsizes)) },
  'Log bandwith [MB/s]' => proc { |b| short(to_mb(b.log_sizes) / b.times) },
  'Wall Time [s]' => proc { |b| short(b.times) },
  'System time [s]' => proc { |b| short(b.system_time / USER_HZ) },
  'User time [s]' => proc { |b| short(b.user_time / USER_HZ) },
  'Time of all CPUs [s]' => proc do |b|
    total_cpu_time = b.time_per_cpu.map(&:sum)
    short(average(total_cpu_time) / 1e9)
  end,
  'Derivation time of all CPUs [s]' => proc do |b|
    per_cpu_time_std = b.time_per_cpu.map(&:standard_deviation)
    short(average(per_cpu_time_std) / 1e9)
  end
}

PERF_EVENTS.each do |field|
    name = field.gsub("-", " ").capitalize
    BENCH_FIELDS[name] = proc do |b| short(average(b.perf_stats[field])) end
end


def main
  benchs = generate(ARGV[0])
  lib_count = 0
  rows = benchs.map do |bench|
    # next if bench.threads != 16

    columns = []
    columns << bench.name
    columns << bench.args.join(' ')
    lib_count = [bench.libs.size, lib_count].max
    priority = {
      'pthread' => 0, 'tthread' => 1, 'pt' => 2, 'inspector' => 3
    }
    sorted = bench.libs.sort { |a, b| priority[a.name] <=> priority[b.name] }
    BENCH_FIELDS.values.each do |column|
      columns << sorted.map { |lib| column.call(lib) }
    end
    columns
  end
  template = Template.new(ERB_TEMPLATE)
  # template.write('table.tex', rows: [header] + rows.compact, columns: %w{l r r r r r r})
  rows = [DEFAULT_HEADER + BENCH_FIELDS.keys] + rows.compact
  template.write('table.html', rows: rows)
end

main
