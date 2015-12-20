require 'json'
require 'erb'
require 'etc'
require 'fileutils'

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

  def covariance(other)
    x_mean = mean
    y_mean = mean
    each_with_index.map do |x, i|
      y = other[i]
      (x - x_mean) * (y - y_mean)
    end.mean
  end
end

LIB_ALIASES = {
  "pthread" => "pthread",
  "pt" =>        "OS support",
  "tthread" =>   "Threading lib.",
  "inspector" => "Total overheads"
}

module HashExtensions
  def subhash(*keys)
    keys = keys.select { |k| key?(k) }
    Hash[keys.zip(values_at(*keys))]
  end
end
Hash.send(:include, HashExtensions)

def average(ary_)
  ary = ary_.dup
  ary.delete_at(ary.find_index(ary.max))
  ary.delete_at(ary.find_index(ary.min))
  ary.map!(&:to_f)
  ary.mean
end

PERF_EVENTS = [
    'sigsegv',
    'sigusr1',
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
    'task-clock',
]

class Measurement
  attr_accessor :options,
                :application,
                :times,
                :log_sizes,
                :log_sizes_all,
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
    bench =~ /^([^-]+)/
    b.name = $1
    b.libs = []
    threads = data["threads"] or
      abort "no threads found in #{bench}"
    next unless threads == 16
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
      m.log_sizes_all = measures["log_sizes"].map(&:to_i)
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

LATEX_TEMPLATE = <<-EOF
\\begin{figure}[t]
\\centering
\\myfontsize
{
\\begin{tabular}{m{1cm}|m{1cm}|m{1.4cm}|m{1.25cm}|m{1.25cm}}
       & \\multicolumn{2}{c|}{ Provenance log details [MB] }   &  Bandwidth & Branch instr. \\\\
   { Application} & Size & Compressed & [MB/sec] &  [Instr/sec] \\\\
  \\hline \\hline
<%- rows.each_with_index do |columns, i| -%>
  <%- if i != 0 -%>
    <%= columns.map do |col|
      if col.is_a? Array
         tex_escape(col[3])
      else
         tex_escape(col)
      end
    end.join("& ") -%> \\\\
  <%- end -%>
<%- end %>
\\hline
\\end{tabular}
}

\\caption{Runtime statistics of benchmarks with 16 threads (Detailed results available here: \\href{https://goo.gl/0wp1kC}{goo.gl/0wp1kC}) }                                                                                                                                      \\label{tab:apps}
\\end{figure}
EOF

HTML_TEMPLATE = <<-EOF
<html>
<head>

<link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" rel="stylesheet" integrity="sha256-7s5uDGW3AHqw6xtJmNNtr+OBRJUlgkNJEo78P4b0yRw= sha512-nNo+yCHEyn0smMxSswnf/OnX6/KwJuZTlNZBjauKhTK0c+zT+q5JOCx0UFhXQ6rJR9jg6Es8gPuD2uZcYDLqSw==" crossorigin="anonymous">

<style>
body { padding-top: 70px; }
.resize {
    height: 700px;
    width: auto;
}
</style>

</head>
<body>

<nav class="navbar navbar-default navbar-fixed-top">
  <div class="container-fluid">
    <div class="navbar-header"> <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-6" aria-expanded="false"> <span class="sr-only">Toggle navigation</span> <span class="icon-bar"></span> <span class="icon-bar"></span> <span class="icon-bar"></span> </button> <a class="navbar-brand" href="#">Additional Results</a></div>

    <div class="collapse navbar-collapse">
      <ul class="nav navbar-nav">
        <li role="presentation"><a class="active" href="#graph1">Execution time for varying threads</a></li>
        <li role="presentation"><a href="#graph2">CPU cycles for varying threads</a></li>
        <li role="presentation"><a href="#graph3">Execution time for 16 threads</a></li>
        <li role="presentation"><a href="#graph4">CPU cycles for 16 threads</a></li>
        <li role="presentation"><a href="#graph5">Time for varying Worksize</a></li>
        <li role="presentation"><a href="#graph6">CPU cycles for varying Worksize</a></li>
        <li role="presentation"><a href="#measurement_table">All Values</a></li>
        <li role="presentation"><a href="#raw_data">Raw data</a></li>
      </ul>
    </div>
  </div>
</nav>

<h2 id="graph1">Execution time for 16 threads</h2>
<img class="resize" src="times-Total_overheads.png"/>

<h2 id="graph2">CPU cycles for varying threads</h2>
<img class="resize" src="cpu-cycles-Total_overheads.png"/>

<h2 id="graph3">Execution time for 16 threads</h2>
<img class="resize" src="times-16-threads.png"/>

<h2 id="graph4">CPU cycles for 16 threads</h2>
<img class="resize" src="cpu-cycles-16-threads.png"/>

<h2 id="graph5">Time for varying Worksize</h2>
<img class="resize" src="worksize-times-Total_overheads.png"/>

<h2 id="graph6">CPU cycles for varying Worksize</h2>
<img class="resize" src="worksize-cpu-cycles-Total_overheads.png"/>

<h2 id="measurement_table">All values</h2>
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

<h2 id="raw_data">Raw JSON data logs</h2>

The raw measurement values written by the benchmark can found here:

<ul>
<li> <a href="increasing-threads.json">increasing-threads.json</a> </li>
<li> <a href="increasing-worksize.json">increasing-worksize.json</a> </li>
</ul>

</body>
</html>
EOF

def to_mb(val)
  val / 1000.0 / 1000
end

def short(v)
  if v > 1e4
    format("%.2E", v)
  elsif v > 100
    format('%.0f',v)
  elsif v > 10
    format('%.1f',v)
  elsif v > 1
    format('%.2f',v)
  else
    format('%.3f',v)
  end
end

HTML_BENCH_FIELDS = {
  'Library' => proc { |b| b.name },
  'Logsize [MB]' => proc { |b| short(to_mb(b.log_sizes)) },
  'Compressed Log (Ratio) [MB]' => proc do |b|
    compressed = to_mb(b.compressed_logsizes)
    log_size = to_mb(b.log_sizes)
    format("%s (%.1fx)", short(compressed), short(log_size/compressed))
  end,
  'Log bandwith [MB/s]' => proc { |b| short(to_mb(b.log_sizes) / b.times) },
  'Segfault/time [1/s]' => proc do |b|
    v = average(b.perf_stats["sigsegv"])
    short(v / b.times)
  end,
  'Branch instructions/time [1/s]' => proc do |b|
    v = average(b.perf_stats["branch-instructions"])
    short(v / b.times)
  end,
  'Wall time [s]' => proc { |b| short(b.times) },
  'System time [s]' => proc { |b| short(b.system_time / USER_HZ) },
  'User time [s]' => proc { |b| short(b.user_time / USER_HZ) },
  'CPU time [s]' => proc do |b|
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
  HTML_BENCH_FIELDS[name] = proc do |b|
    v = average(b.perf_stats[field]).to_i
    if v > 1e4
      format("%.2E", v)
    else
      v
    end
  end
end

bench_alias = {
        "linear_regression" => "linear_reg",
        "matrix_multiply" => "matrix_mul",
        "reverse_index" => "reverve_idx",
        "streamcluster" => "streamcl.",
        "string_match" => "string_ma.",
        "word_count" => "word_c",
        "blackscholes" => "blackscho."
}

LATEX_BENCH_FIELDS = {
  'Application' => proc { |b| bench_alias[b.application] || b.application },
  "Log size [MB]" => proc do |b|
    format("%.0f", short(to_mb(b.log_sizes)))
  end,
  "Compressed Log size [MB]" => proc do |b|
    format("%.1f (%.0fx)", short(to_mb(b.compressed_logsizes)),
           b.log_sizes / b.compressed_logsizes)
  end,
  "Bandwith [MB/s])}" => proc do |b|
    format("%.0f", short(to_mb(b.log_sizes) / b.times))
  end,
  'Branch instr./time [1/s]' => proc do |b|
    v = average(b.perf_stats["branch-instructions"])
    short(v / b.times)
  end
}

def usage
  $stderr.puts("#{$0} log.json table.(tex|html)")
  exit(1)
end

def main
  usage if ARGV.size < 2
  case ARGV[1]
  when /\.tex$/
    template = LATEX_TEMPLATE
    bench_fields = LATEX_BENCH_FIELDS
    single_fields = { }
  when /\.html$/
    template = HTML_TEMPLATE
    bench_fields = HTML_BENCH_FIELDS
    single_fields = {
      'Name' => proc  { |b| b.name },
      'Options' => proc { |b| b.args.join("&nbsp;") },
    }
  else
    usage
  end

  benchs = generate(ARGV[0])
  lib_count = 0
  rows = benchs.map do |bench|
    columns = []
    lib_count = [bench.libs.size, lib_count].max
    priority = {
      'pthread' => 0,
      'tthread' => 1,
      'pt' => 2,
      'inspector' => 3
    }
    sorted = bench.libs.sort do |a, b|
      priority[a.name] <=> priority[b.name]
    end
    sorted.each do |b|
      b.name = LIB_ALIASES[b.name]
      b.name.gsub!(/\s/, "&nbsp;")
    end

    single_fields.each do |name, column|
      columns << column.call(bench)
    end

    bench_fields.values.each do |column|
      columns << sorted.map do |lib|
        lib.application = bench.name
        lib.options = bench.args.join(' ')
        column.call(lib)
      end
    end
    columns
  end
  template = Template.new(template)
  rows = [single_fields.keys + bench_fields.keys] + rows.compact
  template.write(ARGV[1], rows: rows)
end

main
