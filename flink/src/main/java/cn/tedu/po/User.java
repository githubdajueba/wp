package cn.tedu.po;

public class User {
    private String id;
    private String name;
    private String age;
    private String gender;
    private Integer count;

    @Override
    public String toString() {
        return "User{" +
                "gender='" + gender + '\'' +
                ", count=" + count +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
