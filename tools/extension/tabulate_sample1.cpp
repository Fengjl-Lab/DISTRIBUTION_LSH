//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// tools/extension/tabulate_sample1.cpp
//
//===-----------------------------------------------------

#include <tabulate/table.hpp>
using namespace tabulate;

auto main() -> int {

  Table universal_constants;
  universal_constants.format().locale("C");

  universal_constants.add_row({"Quantity", "Value"});
  universal_constants.add_row({"Characteristic impedance of vacuum", "376.730 313 461... Ω"});
  universal_constants.add_row({"Electric constant (permittivity of free space)", "8.854 187 817... × 10⁻¹²F·m⁻¹"});
  universal_constants.add_row({"Magnetic constant (permeability of free space)",
                               "4π × 10⁻⁷ N·A⁻² = 1.2566 370 614... × 10⁻⁶ N·A⁻²"});
  universal_constants.add_row({"Gravitational constant (Newtonian constant of gravitation)",
                               "6.6742(10) × 10⁻¹¹m³·kg⁻¹·s⁻²"});
  universal_constants.add_row({"Planck's constant", "6.626 0693(11) × 10⁻³⁴ J·s"});
  universal_constants.add_row({"Dirac's constant", "1.054 571 68(18) × 10⁻³⁴ J·s"});
  universal_constants.add_row({"Speed of light in vacuum", "299 792 458 m·s⁻¹"});

  universal_constants.format()
      .font_style({FontStyle::bold})
      .border_top(" ")
      .border_bottom(" ")
      .border_left(" ")
      .border_right(" ")
      .corner(" ");

  universal_constants[0].format()
      .padding_top(1)
      .padding_bottom(1)
      .font_align(FontAlign::center)
      .font_style({FontStyle::underline})
      .font_background_color(Color::red);


  universal_constants.column(1).format()
      .font_color(Color::yellow);

  universal_constants[0][1].format()
      .font_background_color(Color::blue)
      .font_color(Color::white);

  std::cout << universal_constants << std::endl;


  Table table;
  table.format().locale("C");
  table.add_row({"This paragraph contains a veryveryveryveryveryverylong word. The long word will "
                 "break and word wrap to the next line.",
                 "This paragraph \nhas embedded '\\n' \ncharacters and\n will break\n exactly "
                 "where\n you want it\n to\n break."});

  table[0][0].format().width(20);
  table[0][1].format().width(50);

  std::cout << table << std::endl;

  Table movies;
  movies.format().locale("C");
  movies.add_row({"S/N", "Movie Name", "Director", "Estimated Budget", "Release Date"});
  movies.add_row({"tt1979376", "Toy Story 4", "Josh Cooley", "$200,000,000", "21 June 2019"});
  movies.add_row({"tt3263904", "Sully", "Clint Eastwood", "$60,000,000", "9 September 2016"});
  movies.add_row({"tt1535109", "Captain Phillips", "Paul Greengrass", "$55,000,000", " 11 October 2013"});

  // center align 'Director' column
  movies.column(2).format()
      .font_align(FontAlign::center);

  // right align 'Estimated Budget' column
  movies.column(3).format()
      .font_align(FontAlign::right);

  // right align 'Release Date' column
  movies.column(4).format()
      .font_align(FontAlign::right);

  // center-align and color header cells
  for (size_t i = 0; i < 5; ++i) {
    movies[0][i].format()
        .font_color(Color::yellow)
        .font_align(FontAlign::center)
        .font_style({FontStyle::bold});
  }

  std::cout << movies << std::endl;
  return 0;
}