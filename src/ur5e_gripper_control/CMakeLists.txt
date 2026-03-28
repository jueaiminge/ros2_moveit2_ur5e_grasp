cmake_minimum_required(VERSION 3.8)
project(ur5e_gripper_control)

# use cpp 17
set(CMAKE_CXX_STANDARD 17)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

include_directories(include)

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(moveit_common REQUIRED)
find_package(moveit_core REQUIRED)
find_package(moveit_ros_planning_interface REQUIRED)
find_package(moveit_ros_planning REQUIRED)
find_package(rclcpp_action REQUIRED)
find_package(control_msgs REQUIRED)
find_package(moveit_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(shape_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(std_msgs REQUIRED)
find_package(trajectory_msgs REQUIRED)
find_package(tf2_ros REQUIRED)

find_package(tf2 REQUIRED)

add_executable(demo src/demo.cpp src/ur5e_gripper.cpp)
ament_target_dependencies(demo
  rclcpp moveit_ros_planning_interface tf2 
  moveit_core moveit_ros_planning control_msgs)

add_executable(simple_pick_place src/simple_pick_place.cpp)
ament_target_dependencies(simple_pick_place
  rclcpp
  rclcpp_action
  tf2
  moveit_core
  moveit_msgs
  geometry_msgs
  shape_msgs
  std_msgs
  control_msgs
  moveit_ros_planning
  moveit_ros_planning_interface)

add_executable(fcl_safety_monitor src/fcl_safety_monitor.cpp)
ament_target_dependencies(fcl_safety_monitor
  rclcpp
  tf2
  tf2_ros
  sensor_msgs
  std_msgs
  trajectory_msgs
  moveit_core
  moveit_msgs
  moveit_ros_planning)

install(TARGETS demo simple_pick_place fcl_safety_monitor
  DESTINATION lib/${PROJECT_NAME}
)

install(
  DIRECTORY 
    launch
    config
  DESTINATION 
    share/${PROJECT_NAME}
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # comment the line when a copyright and license is added to all source files
  set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # comment the line when this package is in a git repo and when
  # a copyright and license is added to all source files
  set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
