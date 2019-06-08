#!/usr/env/ruby

require 'digest'
require 'zlib'

AA = ['A','C','D','E','F','G','H','I','K','L','M','N','P','Q','R','S','T','U','V','W','Y']


def build_all_AA

  hashValues = {}

  AA.each do |a|
    AA.each do |b|
      AA.each do |c|
        AA.each do |d|
          AA.each do |e|
            AA.each do |f|
              AA.each do |g|
                x = "#{a}#{b}#{c}#{d}#{e}#{f}#{g}"
                crc32 = Zlib::crc32(x)
                # hash = Digest::SHA1.hexdigest x
                if hashValues.has_key? crc32
                  # puts "key already exists : #{hbeg},#{hend}"
                  return false
                end
                hashValues[crc32] = true
                puts "#{x}: #{crc32}"
                # puts "#{x}\t#{hash[0..7]}"
              end
            end
          end
        end
      end
    end
  end

  return true

end

if ! build_all_AA
  abort "CRC32 not working"
end
puts "CRC32 works !!"

# i = 0
# while (i+7) <= 39
#   if build_all_AA i,(i+7)
  #     abort "NICE SHA1.hex #{i},#{i+7} Function works !!"
#   else
#     puts "SHA1.hex #{i},#{i+7} not working !!"
#   end
#   i += 1
# end

